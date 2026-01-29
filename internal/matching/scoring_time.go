package matching

import (
	"math"
	"time"
)

// ScoreTimeDelta calculates a score based on the temporal proximity of two timestamps.
// It uses a Gaussian function where:
// - Score is 1.0 when timestamps are identical
// - Score decreases smoothly towards 0 as the time difference increases
// - At delta = windowHours, the score is approximately 0.5
// - Beyond the window, scores become very low (< 0.1)
//
// Parameters:
//   - t1, t2: The two timestamps to compare
//   - windowHours: The time window in hours (at this delta, score ≈ 0.5)
//
// Returns a float64 score in the range [0, 1]
func ScoreTimeDelta(t1, t2 time.Time, windowHours int) float64 {
	if windowHours <= 0 {
		windowHours = 24 // Default to 24 hours
	}

	// Calculate time difference in hours
	timeDiffNanos := math.Abs(float64(t1.Sub(t2)))
	timeDiffHours := timeDiffNanos / float64(time.Hour)

	// Same timestamp (or very close) -> 1.0
	if timeDiffHours == 0 {
		return 1.0
	}

	// Gaussian function: exp(-x² / (2σ²))
	// We want score = 0.5 when timeDiff = windowHours
	// exp(-windowHours² / (2σ²)) = 0.5
	// -windowHours² / (2σ²) = ln(0.5)
	// σ² = -windowHours² / (2 * ln(0.5)) = windowHours² / (2 * ln(2))
	// σ = windowHours / sqrt(2 * ln(2))
	//
	// Using this sigma in the Gaussian:
	// score = exp(-timeDiff² / (2σ²))
	// score = exp(-timeDiff² * ln(2) / windowHours²)
	// score = exp(ln(2) * (-timeDiff² / windowHours²))
	// score = 2^(-timeDiff² / windowHours²)

	window := float64(windowHours)
	exponent := -(timeDiffHours * timeDiffHours) / (window * window)

	// Using the equivalent form: 2^(-x²/w²) = exp(ln(2) * (-x²/w²))
	score := math.Exp(math.Ln2 * exponent)

	return score
}
