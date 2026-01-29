package matching

// FeatureScore represents the score for a single feature in the scoring calculation.
type FeatureScore struct {
	Name  string  `json:"name"`
	Score float64 `json:"score"`
}

// RejectedCandidate represents a candidate that was evaluated but not selected.
type RejectedCandidate struct {
	TransactionID   string  `json:"transactionId"`
	Score           float64 `json:"score"`
	ExclusionReason string  `json:"exclusionReason"`
}

// ThresholdApplied represents the thresholds used in decision making.
type ThresholdApplied struct {
	AutoMatch float64 `json:"autoMatch"`
	Review    float64 `json:"review"`
}

// Explanation contains detailed documentation of score calculation for audit purposes.
type Explanation struct {
	TotalScore         float64             `json:"totalScore"`
	Features           map[string]float64  `json:"features"`
	WeightsUsed        map[string]float64  `json:"weightsUsed"`
	ThresholdApplied   ThresholdApplied    `json:"thresholdApplied"`
	DecisionReason     string              `json:"decisionReason"`
	RejectedCandidates []RejectedCandidate `json:"rejectedCandidates,omitempty"`
}

// NewExplanation creates a new Explanation with the given parameters.
func NewExplanation(
	totalScore float64,
	features map[string]float64,
	weightsUsed map[string]float64,
	thresholds Thresholds,
	decision Decision,
) Explanation {
	return Explanation{
		TotalScore:         totalScore,
		Features:           features,
		WeightsUsed:        weightsUsed,
		ThresholdApplied:   ThresholdApplied(thresholds),
		DecisionReason:     generateDecisionReason(totalScore, thresholds, decision),
		RejectedCandidates: []RejectedCandidate{},
	}
}

// AddRejectedCandidate adds a rejected candidate to the explanation.
func (e *Explanation) AddRejectedCandidate(transactionID string, score float64, reason string) {
	e.RejectedCandidates = append(e.RejectedCandidates, RejectedCandidate{
		TransactionID:   transactionID,
		Score:           score,
		ExclusionReason: reason,
	})
}

// generateDecisionReason generates a human-readable reason for the decision.
func generateDecisionReason(totalScore float64, thresholds Thresholds, decision Decision) string {
	switch decision {
	case DecisionMatched:
		return "Score meets or exceeds auto-match threshold"
	case DecisionRequiresReview:
		return "Score meets review threshold but below auto-match threshold"
	case DecisionUnmatched:
		return "Score below review threshold"
	default:
		return "Unknown decision"
	}
}
