package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScoreAmountDelta_EqualAmounts(t *testing.T) {
	// Equal amounts should always return 1.0
	score := ScoreAmountDelta(100.0, 100.0, 1.0, 0.0)
	assert.Equal(t, 1.0, score, "equal amounts should return 1.0")
}

func TestScoreAmountDelta_Delta05PercentWithTolerance1Percent(t *testing.T) {
	// Amount1: 100, Amount2: 100.5 -> diff = 0.5
	// Tolerance: 1% of 100.5 = 1.005
	// Score = 1 - (0.5 / 1.005) ≈ 0.502
	score := ScoreAmountDelta(100.0, 100.5, 1.0, 0.0)
	assert.InDelta(t, 0.5, score, 0.05, "delta 0.5% with tolerance 1% should be ~0.5")
}

func TestScoreAmountDelta_Delta1PercentWithTolerance1Percent(t *testing.T) {
	// Amount1: 100, Amount2: 101 -> diff = 1
	// Tolerance: 1% of 101 = 1.01
	// Score = 1 - (1 / 1.01) ≈ 0.0099
	score := ScoreAmountDelta(100.0, 101.0, 1.0, 0.0)
	assert.InDelta(t, 0.0, score, 0.02, "delta 1% with tolerance 1% should be ~0.0")
}

func TestScoreAmountDelta_DeltaGreaterThanTolerance(t *testing.T) {
	// Amount1: 100, Amount2: 105 -> diff = 5
	// Tolerance: 1% of 105 = 1.05
	// Score = 0 (diff > tolerance)
	score := ScoreAmountDelta(100.0, 105.0, 1.0, 0.0)
	assert.Equal(t, 0.0, score, "delta > tolerance should return 0.0")
}

func TestScoreAmountDelta_AbsoluteTolerance(t *testing.T) {
	// Amount1: 100, Amount2: 101 -> diff = 1
	// Absolute tolerance: 2 EUR
	// Score = 1 - (1 / 2) = 0.5
	score := ScoreAmountDelta(100.0, 101.0, 0.0, 2.0)
	assert.InDelta(t, 0.5, score, 0.01, "delta 1EUR with absolute tolerance 2EUR should be ~0.5")
}

func TestScoreAmountDelta_CombinationPercentAndAbsolute(t *testing.T) {
	// Test that max of both tolerances is used
	// Amount1: 100, Amount2: 102 -> diff = 2
	// Percent tolerance: 1% of 102 = 1.02
	// Absolute tolerance: 4.0
	// Effective tolerance: max(1.02, 4.0) = 4.0
	// Score = 1 - (2 / 4) = 0.5
	score := ScoreAmountDelta(100.0, 102.0, 1.0, 4.0)
	assert.InDelta(t, 0.5, score, 0.01, "combination of tolerances should use max")

	// Now test where percent is larger
	// Amount1: 1000, Amount2: 1005 -> diff = 5
	// Percent tolerance: 1% of 1005 = 10.05
	// Absolute tolerance: 2.0
	// Effective tolerance: max(10.05, 2.0) = 10.05
	// Score = 1 - (5 / 10.05) ≈ 0.502
	score2 := ScoreAmountDelta(1000.0, 1005.0, 1.0, 2.0)
	assert.InDelta(t, 0.5, score2, 0.05, "combination should use percent tolerance when larger")
}

func TestScoreAmountDelta_NegativeAmounts(t *testing.T) {
	// Both negative, equal
	score := ScoreAmountDelta(-100.0, -100.0, 1.0, 0.0)
	assert.Equal(t, 1.0, score, "equal negative amounts should return 1.0")

	// Both negative, with diff
	// Amount1: -100, Amount2: -100.5 -> diff = 0.5
	// Tolerance: 1% of 100.5 = 1.005
	// Score = 1 - (0.5 / 1.005) ≈ 0.502
	score2 := ScoreAmountDelta(-100.0, -100.5, 1.0, 0.0)
	assert.InDelta(t, 0.5, score2, 0.05, "negative amounts with delta 0.5% should be ~0.5")

	// One positive, one negative
	// Amount1: 100, Amount2: -100 -> diff = 200
	// Tolerance: 1% of 100 = 1
	// Score = 0 (diff > tolerance)
	score3 := ScoreAmountDelta(100.0, -100.0, 1.0, 0.0)
	assert.Equal(t, 0.0, score3, "opposite signs with small tolerance should return 0.0")
}

func TestScoreAmountDelta_ZeroAmounts(t *testing.T) {
	// Both zero
	score := ScoreAmountDelta(0.0, 0.0, 1.0, 0.0)
	assert.Equal(t, 1.0, score, "both zero should return 1.0")

	// One zero with absolute tolerance
	// Amount1: 0, Amount2: 1 -> diff = 1
	// Percent tolerance: 1% of 1 = 0.01
	// Absolute tolerance: 2.0
	// Effective tolerance: max(0.01, 2.0) = 2.0
	// Score = 1 - (1 / 2) = 0.5
	score2 := ScoreAmountDelta(0.0, 1.0, 1.0, 2.0)
	assert.InDelta(t, 0.5, score2, 0.01, "zero with non-zero and absolute tolerance should work")
}

func TestScoreAmountDelta_SymmetricOrder(t *testing.T) {
	scoreForward := ScoreAmountDelta(100.0, 105.0, 5.0, 0.0)
	scoreBackward := ScoreAmountDelta(105.0, 100.0, 5.0, 0.0)
	assert.Equal(t, scoreForward, scoreBackward, "score should be same regardless of amount order")
}

func TestScoreAmountDelta_LinearDecay(t *testing.T) {
	// Verify linear decay: score decreases linearly from 1 to 0
	// Amount base: 100, Tolerance: 10%
	// Note: tolerance is 10% of the max(abs(a1), abs(a2))
	// For 100 vs 100+diff, tolerance = 10% of (100+diff)
	tests := []struct {
		diff        float64
		expectedMin float64
		expectedMax float64
		description string
	}{
		{0, 1.0, 1.0, "no difference"},
		{2.5, 0.74, 0.78, "small diff"},  // tol=10.25, score=1-(2.5/10.25)≈0.756
		{5.0, 0.51, 0.55, "medium diff"}, // tol=10.5, score=1-(5/10.5)≈0.524
		{7.5, 0.28, 0.32, "large diff"},  // tol=10.75, score=1-(7.5/10.75)≈0.302
		{10.0, 0.08, 0.12, "near limit"}, // tol=11.0, score=1-(10/11)≈0.091
		{15.0, 0.0, 0.0, "beyond tolerance"},
	}

	var previousScore = 2.0
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			score := ScoreAmountDelta(100.0, 100.0+tt.diff, 10.0, 0.0)

			assert.GreaterOrEqual(t, score, tt.expectedMin,
				"%s: score %.4f should be >= %.4f", tt.description, score, tt.expectedMin)
			assert.LessOrEqual(t, score, tt.expectedMax,
				"%s: score %.4f should be <= %.4f", tt.description, score, tt.expectedMax)

			// Verify monotonic decrease
			if tt.diff > 0 {
				assert.Less(t, score, previousScore,
					"score should decrease as diff increases")
			}
			previousScore = score
		})
	}
}

func TestScoreAmountDelta_NoTolerance(t *testing.T) {
	// With zero tolerance, only exact match should score 1.0
	score := ScoreAmountDelta(100.0, 100.0, 0.0, 0.0)
	assert.Equal(t, 1.0, score, "exact match with no tolerance should return 1.0")

	score2 := ScoreAmountDelta(100.0, 100.01, 0.0, 0.0)
	assert.Equal(t, 0.0, score2, "any difference with no tolerance should return 0.0")
}

func TestScoreAmountDelta_VerySmallDifference(t *testing.T) {
	// Very small difference should give score very close to 1.0
	score := ScoreAmountDelta(100.0, 100.0001, 1.0, 0.0)
	assert.Greater(t, score, 0.999, "very small difference should give score very close to 1.0")
}

func TestScoreAmountDelta_LargeAmounts(t *testing.T) {
	// Test with large amounts
	// Amount1: 1000000, Amount2: 1005000 -> diff = 5000
	// Tolerance: 1% of 1005000 = 10050
	// Score = 1 - (5000 / 10050) ≈ 0.502
	score := ScoreAmountDelta(1000000.0, 1005000.0, 1.0, 0.0)
	assert.InDelta(t, 0.5, score, 0.05, "large amounts should work correctly")
}
