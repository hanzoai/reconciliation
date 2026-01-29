package matching

// Thresholds defines the score thresholds for decision making.
type Thresholds struct {
	AutoMatch float64 // Score >= AutoMatch -> MATCHED
	Review    float64 // Score >= Review && Score < AutoMatch -> REQUIRES_REVIEW
}

// DefaultThresholds returns the default thresholds for decision making.
// AutoMatch: 0.85 - scores at or above this threshold result in automatic match
// Review: 0.60 - scores at or above this threshold (but below AutoMatch) require review
func DefaultThresholds() Thresholds {
	return Thresholds{
		AutoMatch: 0.85,
		Review:    0.60,
	}
}

// DecideFromScore transforms a score into a decision based on the provided thresholds.
//
// Decision logic:
//   - score >= thresholds.AutoMatch -> MATCHED
//   - score >= thresholds.Review && score < thresholds.AutoMatch -> REQUIRES_REVIEW
//   - score < thresholds.Review -> UNMATCHED
func DecideFromScore(score float64, thresholds Thresholds) Decision {
	if score >= thresholds.AutoMatch {
		return DecisionMatched
	}
	if score >= thresholds.Review {
		return DecisionRequiresReview
	}
	return DecisionUnmatched
}
