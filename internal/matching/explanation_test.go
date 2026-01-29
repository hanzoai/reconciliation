package matching

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExplanation_ContainsAllFields(t *testing.T) {
	t.Parallel()

	features := map[string]float64{
		"timestamp": 0.95,
		"amount":    0.88,
		"metadata":  0.75,
	}
	weights := map[string]float64{
		"timestamp": 0.3,
		"amount":    0.4,
		"metadata":  0.3,
	}
	thresholds := Thresholds{
		AutoMatch: 0.85,
		Review:    0.60,
	}

	explanation := NewExplanation(0.87, features, weights, thresholds, DecisionMatched)

	// Verify all required fields are present
	assert.Equal(t, 0.87, explanation.TotalScore)
	assert.Equal(t, features, explanation.Features)
	assert.Equal(t, weights, explanation.WeightsUsed)
	assert.Equal(t, 0.85, explanation.ThresholdApplied.AutoMatch)
	assert.Equal(t, 0.60, explanation.ThresholdApplied.Review)
	assert.NotEmpty(t, explanation.DecisionReason)
	assert.NotNil(t, explanation.RejectedCandidates)
}

func TestExplanation_JSONSerialization(t *testing.T) {
	t.Parallel()

	features := map[string]float64{
		"timestamp": 0.95,
		"amount":    0.88,
		"metadata":  0.75,
	}
	weights := map[string]float64{
		"timestamp": 0.3,
		"amount":    0.4,
		"metadata":  0.3,
	}
	thresholds := Thresholds{
		AutoMatch: 0.85,
		Review:    0.60,
	}

	explanation := NewExplanation(0.87, features, weights, thresholds, DecisionMatched)
	explanation.AddRejectedCandidate("tx-123", 0.55, "Score below review threshold")

	data, err := json.Marshal(explanation)
	require.NoError(t, err)

	// Verify JSON contains expected fields
	var jsonMap map[string]interface{}
	err = json.Unmarshal(data, &jsonMap)
	require.NoError(t, err)

	assert.Contains(t, jsonMap, "totalScore")
	assert.Contains(t, jsonMap, "features")
	assert.Contains(t, jsonMap, "weightsUsed")
	assert.Contains(t, jsonMap, "thresholdApplied")
	assert.Contains(t, jsonMap, "decisionReason")
	assert.Contains(t, jsonMap, "rejectedCandidates")
}

func TestExplanation_JSONDeserialization(t *testing.T) {
	t.Parallel()

	jsonData := `{
		"totalScore": 0.87,
		"features": {
			"timestamp": 0.95,
			"amount": 0.88,
			"metadata": 0.75
		},
		"weightsUsed": {
			"timestamp": 0.3,
			"amount": 0.4,
			"metadata": 0.3
		},
		"thresholdApplied": {
			"autoMatch": 0.85,
			"review": 0.60
		},
		"decisionReason": "Score meets or exceeds auto-match threshold",
		"rejectedCandidates": [
			{
				"transactionId": "tx-123",
				"score": 0.55,
				"exclusionReason": "Score below review threshold"
			}
		]
	}`

	var explanation Explanation
	err := json.Unmarshal([]byte(jsonData), &explanation)
	require.NoError(t, err)

	assert.Equal(t, 0.87, explanation.TotalScore)
	assert.Equal(t, 0.95, explanation.Features["timestamp"])
	assert.Equal(t, 0.88, explanation.Features["amount"])
	assert.Equal(t, 0.75, explanation.Features["metadata"])
	assert.Equal(t, 0.3, explanation.WeightsUsed["timestamp"])
	assert.Equal(t, 0.4, explanation.WeightsUsed["amount"])
	assert.Equal(t, 0.3, explanation.WeightsUsed["metadata"])
	assert.Equal(t, 0.85, explanation.ThresholdApplied.AutoMatch)
	assert.Equal(t, 0.60, explanation.ThresholdApplied.Review)
	assert.Equal(t, "Score meets or exceeds auto-match threshold", explanation.DecisionReason)
	require.Len(t, explanation.RejectedCandidates, 1)
	assert.Equal(t, "tx-123", explanation.RejectedCandidates[0].TransactionID)
	assert.Equal(t, 0.55, explanation.RejectedCandidates[0].Score)
	assert.Equal(t, "Score below review threshold", explanation.RejectedCandidates[0].ExclusionReason)
}

func TestExplanation_RejectedCandidatesPresentWithReasons(t *testing.T) {
	t.Parallel()

	features := map[string]float64{
		"timestamp": 0.95,
		"amount":    0.88,
	}
	weights := map[string]float64{
		"timestamp": 0.3,
		"amount":    0.7,
	}
	thresholds := DefaultThresholds()

	explanation := NewExplanation(0.90, features, weights, thresholds, DecisionMatched)

	// Add multiple rejected candidates with different reasons
	explanation.AddRejectedCandidate("tx-001", 0.55, "Score below review threshold")
	explanation.AddRejectedCandidate("tx-002", 0.70, "Higher scoring candidate selected")
	explanation.AddRejectedCandidate("tx-003", 0.45, "Amount difference exceeds tolerance")

	require.Len(t, explanation.RejectedCandidates, 3)

	// Verify first rejected candidate
	assert.Equal(t, "tx-001", explanation.RejectedCandidates[0].TransactionID)
	assert.Equal(t, 0.55, explanation.RejectedCandidates[0].Score)
	assert.Equal(t, "Score below review threshold", explanation.RejectedCandidates[0].ExclusionReason)

	// Verify second rejected candidate
	assert.Equal(t, "tx-002", explanation.RejectedCandidates[1].TransactionID)
	assert.Equal(t, 0.70, explanation.RejectedCandidates[1].Score)
	assert.Equal(t, "Higher scoring candidate selected", explanation.RejectedCandidates[1].ExclusionReason)

	// Verify third rejected candidate
	assert.Equal(t, "tx-003", explanation.RejectedCandidates[2].TransactionID)
	assert.Equal(t, 0.45, explanation.RejectedCandidates[2].Score)
	assert.Equal(t, "Amount difference exceeds tolerance", explanation.RejectedCandidates[2].ExclusionReason)
}

func TestExplanation_DecisionReasonConsistentWithDecision(t *testing.T) {
	t.Parallel()

	features := map[string]float64{"amount": 0.90}
	weights := map[string]float64{"amount": 1.0}
	thresholds := DefaultThresholds()

	tests := []struct {
		name           string
		score          float64
		decision       Decision
		expectedReason string
	}{
		{
			name:           "MATCHED decision reason",
			score:          0.90,
			decision:       DecisionMatched,
			expectedReason: "Score meets or exceeds auto-match threshold",
		},
		{
			name:           "REQUIRES_REVIEW decision reason",
			score:          0.75,
			decision:       DecisionRequiresReview,
			expectedReason: "Score meets review threshold but below auto-match threshold",
		},
		{
			name:           "UNMATCHED decision reason",
			score:          0.50,
			decision:       DecisionUnmatched,
			expectedReason: "Score below review threshold",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			explanation := NewExplanation(tt.score, features, weights, thresholds, tt.decision)
			assert.Equal(t, tt.expectedReason, explanation.DecisionReason)
		})
	}
}

func TestExplanation_JSONSerializationRoundTrip(t *testing.T) {
	t.Parallel()

	features := map[string]float64{
		"timestamp": 0.95,
		"amount":    0.88,
		"metadata":  0.75,
	}
	weights := map[string]float64{
		"timestamp": 0.3,
		"amount":    0.4,
		"metadata":  0.3,
	}
	thresholds := Thresholds{
		AutoMatch: 0.85,
		Review:    0.60,
	}

	original := NewExplanation(0.87, features, weights, thresholds, DecisionMatched)
	original.AddRejectedCandidate("tx-456", 0.40, "Score too low")
	original.AddRejectedCandidate("tx-789", 0.55, "Below review threshold")

	// Serialize
	data, err := json.Marshal(original)
	require.NoError(t, err)

	// Deserialize
	var decoded Explanation
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	// Verify all fields match
	assert.Equal(t, original.TotalScore, decoded.TotalScore)
	assert.Equal(t, original.Features, decoded.Features)
	assert.Equal(t, original.WeightsUsed, decoded.WeightsUsed)
	assert.Equal(t, original.ThresholdApplied.AutoMatch, decoded.ThresholdApplied.AutoMatch)
	assert.Equal(t, original.ThresholdApplied.Review, decoded.ThresholdApplied.Review)
	assert.Equal(t, original.DecisionReason, decoded.DecisionReason)
	require.Len(t, decoded.RejectedCandidates, 2)
	assert.Equal(t, original.RejectedCandidates[0], decoded.RejectedCandidates[0])
	assert.Equal(t, original.RejectedCandidates[1], decoded.RejectedCandidates[1])
}

func TestExplanation_EmptyRejectedCandidates(t *testing.T) {
	t.Parallel()

	features := map[string]float64{"amount": 0.90}
	weights := map[string]float64{"amount": 1.0}
	thresholds := DefaultThresholds()

	explanation := NewExplanation(0.90, features, weights, thresholds, DecisionMatched)

	// RejectedCandidates should be initialized as empty slice, not nil
	assert.NotNil(t, explanation.RejectedCandidates)
	assert.Len(t, explanation.RejectedCandidates, 0)

	// JSON should omit empty rejected candidates
	data, err := json.Marshal(explanation)
	require.NoError(t, err)

	var jsonMap map[string]interface{}
	err = json.Unmarshal(data, &jsonMap)
	require.NoError(t, err)

	// Empty slices with omitempty should not be present
	_, hasRejected := jsonMap["rejectedCandidates"]
	assert.False(t, hasRejected, "Empty rejectedCandidates should be omitted from JSON")
}

func TestRejectedCandidate_JSONSerialization(t *testing.T) {
	t.Parallel()

	candidate := RejectedCandidate{
		TransactionID:   "tx-abc-123",
		Score:           0.45,
		ExclusionReason: "Amount difference too large",
	}

	data, err := json.Marshal(candidate)
	require.NoError(t, err)

	var decoded RejectedCandidate
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, candidate.TransactionID, decoded.TransactionID)
	assert.Equal(t, candidate.Score, decoded.Score)
	assert.Equal(t, candidate.ExclusionReason, decoded.ExclusionReason)
}

func TestThresholdApplied_JSONSerialization(t *testing.T) {
	t.Parallel()

	threshold := ThresholdApplied{
		AutoMatch: 0.85,
		Review:    0.60,
	}

	data, err := json.Marshal(threshold)
	require.NoError(t, err)

	var decoded ThresholdApplied
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, threshold.AutoMatch, decoded.AutoMatch)
	assert.Equal(t, threshold.Review, decoded.Review)
}
