package matching

import (
	"math"
)

// ScoreAmountDelta calculates a score based on amount proximity.
// It uses a linear decay function where:
// - Score is 1.0 when amounts are identical
// - Score decreases linearly towards 0 as the difference increases
// - Score becomes 0 when difference exceeds tolerance
//
// The tolerance is calculated as the maximum of:
// - tolerancePercent of the larger amount (as percentage, e.g., 1.0 = 1%)
// - toleranceAbsolute (fixed amount)
//
// Parameters:
//   - a1, a2: The two amounts to compare
//   - tolerancePercent: Percentage tolerance (1.0 = 1%)
//   - toleranceAbsolute: Absolute tolerance value
//
// Returns a float64 score in the range [0, 1]
func ScoreAmountDelta(a1, a2, tolerancePercent, toleranceAbsolute float64) float64 {
	// Calculate absolute difference
	diff := math.Abs(a1 - a2)

	// Exact match
	if diff == 0 {
		return 1.0
	}

	// Calculate percentage-based tolerance using the larger absolute amount
	maxAmount := math.Max(math.Abs(a1), math.Abs(a2))
	percentTolerance := maxAmount * (tolerancePercent / 100.0)

	// Use the maximum of percentage and absolute tolerance
	effectiveTolerance := math.Max(percentTolerance, toleranceAbsolute)

	// If no tolerance configured, only exact match scores 1.0
	if effectiveTolerance <= 0 {
		return 0.0
	}

	// Beyond tolerance -> 0
	if diff >= effectiveTolerance {
		return 0.0
	}

	// Linear decay: score = 1 - (diff / tolerance)
	score := 1.0 - (diff / effectiveTolerance)

	return score
}
