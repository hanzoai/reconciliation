package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestScoreTimeDelta_SameTimestamp(t *testing.T) {
	now := time.Now()
	score := ScoreTimeDelta(now, now, 72)
	assert.Equal(t, 1.0, score, "same timestamp should return 1.0")
}

func TestScoreTimeDelta_Delta1HourWindow72H(t *testing.T) {
	now := time.Now()
	oneHourLater := now.Add(1 * time.Hour)

	score := ScoreTimeDelta(now, oneHourLater, 72)
	assert.InDelta(t, 0.99, score, 0.01, "delta 1h with window 72h should be ~0.99")
}

func TestScoreTimeDelta_Delta24HoursWindow72H(t *testing.T) {
	now := time.Now()
	dayLater := now.Add(24 * time.Hour)

	score := ScoreTimeDelta(now, dayLater, 72)
	// Gaussian decay: at 1/3 of window, score should be high but below 1.0
	// Mathematical result: 2^(-(24/72)²) = 2^(-1/9) ≈ 0.926
	assert.Greater(t, score, 0.8, "delta 24h with window 72h should be > 0.8")
	assert.Less(t, score, 1.0, "delta 24h with window 72h should be < 1.0")
}

func TestScoreTimeDelta_DeltaEqualsWindow(t *testing.T) {
	now := time.Now()
	windowLater := now.Add(72 * time.Hour)

	score := ScoreTimeDelta(now, windowLater, 72)
	assert.InDelta(t, 0.5, score, 0.01, "delta 72h with window 72h should be ~0.5")
}

func TestScoreTimeDelta_DeltaGreaterThanWindow(t *testing.T) {
	now := time.Now()
	beyondWindow := now.Add(144 * time.Hour) // 2x window

	score := ScoreTimeDelta(now, beyondWindow, 72)
	assert.Less(t, score, 0.1, "delta > window should have very low score (< 0.1)")
}

func TestScoreTimeDelta_DifferentWindowsProportionalScores(t *testing.T) {
	now := time.Now()

	// Same absolute delta (24h), different windows
	delta24h := now.Add(24 * time.Hour)

	// With 24h window, delta equals window -> ~0.5
	score24hWindow := ScoreTimeDelta(now, delta24h, 24)
	assert.InDelta(t, 0.5, score24hWindow, 0.01, "delta 24h with window 24h should be ~0.5")

	// With 48h window, delta is half of window -> higher score
	score48hWindow := ScoreTimeDelta(now, delta24h, 48)
	assert.Greater(t, score48hWindow, score24hWindow, "larger window should give higher score for same delta")
	assert.InDelta(t, 0.84, score48hWindow, 0.05, "delta 24h with window 48h should be ~0.84")

	// With 72h window, delta is 1/3 of window -> even higher score
	score72hWindow := ScoreTimeDelta(now, delta24h, 72)
	assert.Greater(t, score72hWindow, score48hWindow, "even larger window should give even higher score")
}

func TestScoreTimeDelta_SymmetricOrder(t *testing.T) {
	now := time.Now()
	later := now.Add(12 * time.Hour)

	scoreForward := ScoreTimeDelta(now, later, 24)
	scoreBackward := ScoreTimeDelta(later, now, 24)

	assert.Equal(t, scoreForward, scoreBackward, "score should be same regardless of time order")
}

func TestScoreTimeDelta_DefaultWindow(t *testing.T) {
	now := time.Now()
	dayLater := now.Add(24 * time.Hour)

	// Window of 0 should default to 24
	scoreZeroWindow := ScoreTimeDelta(now, dayLater, 0)
	score24Window := ScoreTimeDelta(now, dayLater, 24)

	assert.Equal(t, score24Window, scoreZeroWindow, "zero window should default to 24 hours")
	assert.InDelta(t, 0.5, scoreZeroWindow, 0.01, "delta equal to default window should be ~0.5")
}

func TestScoreTimeDelta_NegativeWindow(t *testing.T) {
	now := time.Now()
	dayLater := now.Add(24 * time.Hour)

	// Negative window should default to 24
	scoreNegativeWindow := ScoreTimeDelta(now, dayLater, -10)
	score24Window := ScoreTimeDelta(now, dayLater, 24)

	assert.Equal(t, score24Window, scoreNegativeWindow, "negative window should default to 24 hours")
}

func TestScoreTimeDelta_VerySmallDelta(t *testing.T) {
	now := time.Now()
	almostNow := now.Add(1 * time.Minute)

	score := ScoreTimeDelta(now, almostNow, 72)
	assert.Greater(t, score, 0.999, "very small delta should give score very close to 1.0")
}

func TestScoreTimeDelta_GaussianDecay(t *testing.T) {
	// Verify the Gaussian decay property: scores decrease smoothly
	// Formula: score = 2^(-(delta/window)²)
	// This ensures score = 0.5 when delta = window
	now := time.Now()
	windowHours := 72

	tests := []struct {
		deltaHours  int
		expectedMin float64
		expectedMax float64
		description string
	}{
		{0, 1.0, 1.0, "same timestamp"},
		{1, 0.99, 1.0, "1 hour delta"},
		{12, 0.97, 0.99, "12 hours delta"},
		{24, 0.92, 0.94, "24 hours delta"},
		{36, 0.83, 0.86, "36 hours delta"},
		{48, 0.72, 0.76, "48 hours delta"},
		{72, 0.49, 0.51, "72 hours delta (window)"},
		{96, 0.29, 0.32, "96 hours delta"},
		{144, 0.05, 0.08, "144 hours delta (2x window)"},
	}

	var previousScore = 2.0 // Start higher than max
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			future := now.Add(time.Duration(tt.deltaHours) * time.Hour)
			score := ScoreTimeDelta(now, future, windowHours)

			assert.GreaterOrEqual(t, score, tt.expectedMin,
				"%s: score %.4f should be >= %.4f", tt.description, score, tt.expectedMin)
			assert.LessOrEqual(t, score, tt.expectedMax,
				"%s: score %.4f should be <= %.4f", tt.description, score, tt.expectedMax)

			// Verify monotonic decrease
			if tt.deltaHours > 0 {
				assert.Less(t, score, previousScore,
					"score should decrease as delta increases")
			}
			previousScore = score
		})
	}
}
