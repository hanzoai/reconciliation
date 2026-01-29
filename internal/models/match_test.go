package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecision_IsValid(t *testing.T) {
	tests := []struct {
		decision Decision
		expected bool
	}{
		{DecisionMatched, true},
		{DecisionRequiresReview, true},
		{DecisionUnmatched, true},
		{DecisionManual, true},
		{DecisionManualMatch, true},
		{"INVALID", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(string(tt.decision), func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.decision.IsValid())
		})
	}
}

func TestDecision_String(t *testing.T) {
	assert.Equal(t, "MATCHED", DecisionMatched.String())
	assert.Equal(t, "REQUIRES_REVIEW", DecisionRequiresReview.String())
	assert.Equal(t, "UNMATCHED", DecisionUnmatched.String())
	assert.Equal(t, "MANUAL", DecisionManual.String())
	assert.Equal(t, "MANUAL_MATCH", DecisionManualMatch.String())
}

func TestExplanation_JSONSerialization(t *testing.T) {
	explanation := Explanation{
		Reason: "Exact match on external_id",
		Score:  1.0,
		FieldNotes: map[string]string{
			"external_id": "exact match",
			"amount":      "exact match",
		},
		Details: map[string]interface{}{
			"matchedFields": []string{"external_id", "amount", "currency"},
		},
	}

	data, err := json.Marshal(explanation)
	require.NoError(t, err)

	var decoded Explanation
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, explanation.Reason, decoded.Reason)
	assert.Equal(t, explanation.Score, decoded.Score)
	assert.Equal(t, explanation.FieldNotes["external_id"], decoded.FieldNotes["external_id"])
	assert.Equal(t, explanation.FieldNotes["amount"], decoded.FieldNotes["amount"])
}

func TestExplanation_JSONDeserialization(t *testing.T) {
	jsonData := `{
		"reason": "Partial match with high confidence",
		"score": 0.85,
		"fieldNotes": {
			"external_id": "exact match",
			"amount": "within tolerance"
		},
		"details": {
			"tolerance": 0.01,
			"confidence": "high"
		}
	}`

	var explanation Explanation
	err := json.Unmarshal([]byte(jsonData), &explanation)
	require.NoError(t, err)

	assert.Equal(t, "Partial match with high confidence", explanation.Reason)
	assert.Equal(t, 0.85, explanation.Score)
	assert.Equal(t, "exact match", explanation.FieldNotes["external_id"])
	assert.Equal(t, "within tolerance", explanation.FieldNotes["amount"])
	assert.Equal(t, 0.01, explanation.Details["tolerance"])
	assert.Equal(t, "high", explanation.Details["confidence"])
}

func TestExplanation_JSONSerialization_EmptyFields(t *testing.T) {
	explanation := Explanation{
		Reason: "No match found",
	}

	data, err := json.Marshal(explanation)
	require.NoError(t, err)

	var decoded Explanation
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, "No match found", decoded.Reason)
	assert.Equal(t, float64(0), decoded.Score)
	assert.Nil(t, decoded.FieldNotes)
	assert.Nil(t, decoded.Details)
}
