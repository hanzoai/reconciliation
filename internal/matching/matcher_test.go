package matching

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptrUUIDMatcher(u uuid.UUID) *uuid.UUID {
	return &u
}

func TestDecision_IsValid(t *testing.T) {
	tests := []struct {
		decision Decision
		expected bool
	}{
		{DecisionMatched, true},
		{DecisionRequiresReview, true},
		{DecisionUnmatched, true},
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
}

func TestCandidate_JSONSerialization(t *testing.T) {
	tx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUIDMatcher(uuid.New()),
		Side:       models.TransactionSideLedger,
		Provider:   "stripe",
		ExternalID: "tx_123",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now().UTC().Truncate(time.Microsecond),
		IngestedAt: time.Now().UTC().Truncate(time.Microsecond),
		Metadata:   map[string]interface{}{"key": "value"},
	}

	candidate := Candidate{
		Transaction: tx,
		Score:       0.95,
		Reasons:     []string{"exact amount match", "same currency", "date within tolerance"},
	}

	data, err := json.Marshal(candidate)
	require.NoError(t, err)

	var decoded Candidate
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, candidate.Score, decoded.Score)
	assert.Equal(t, candidate.Reasons, decoded.Reasons)
	assert.Equal(t, candidate.Transaction.ID, decoded.Transaction.ID)
	assert.Equal(t, candidate.Transaction.Amount, decoded.Transaction.Amount)
	assert.Equal(t, candidate.Transaction.Currency, decoded.Transaction.Currency)
	assert.Equal(t, candidate.Transaction.ExternalID, decoded.Transaction.ExternalID)
}

func TestCandidate_JSONSerialization_EmptyReasons(t *testing.T) {
	candidate := Candidate{
		Transaction: &models.Transaction{
			ID:         uuid.New(),
			PolicyID:   ptrUUIDMatcher(uuid.New()),
			Side:       models.TransactionSidePayments,
			Provider:   "adyen",
			ExternalID: "pay_456",
			Amount:     500,
			Currency:   "EUR",
			OccurredAt: time.Now().UTC(),
			IngestedAt: time.Now().UTC(),
		},
		Score:   0.5,
		Reasons: []string{},
	}

	data, err := json.Marshal(candidate)
	require.NoError(t, err)

	var decoded Candidate
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, candidate.Score, decoded.Score)
	assert.Empty(t, decoded.Reasons)
}

func TestCandidate_JSONDeserialization(t *testing.T) {
	txID := uuid.New()
	policyID := uuid.New()
	jsonData := `{
		"transaction": {
			"id": "` + txID.String() + `",
			"policyID": "` + policyID.String() + `",
			"side": "LEDGER",
			"provider": "stripe",
			"externalID": "tx_789",
			"amount": 2500,
			"currency": "GBP",
			"occurredAt": "2024-01-15T10:30:00Z",
			"ingestedAt": "2024-01-15T10:31:00Z"
		},
		"score": 0.78,
		"reasons": ["amount within 5%", "matching reference"]
	}`

	var candidate Candidate
	err := json.Unmarshal([]byte(jsonData), &candidate)
	require.NoError(t, err)

	assert.Equal(t, txID, candidate.Transaction.ID)
	require.NotNil(t, candidate.Transaction.PolicyID)
	assert.Equal(t, policyID, *candidate.Transaction.PolicyID)
	assert.Equal(t, models.TransactionSideLedger, candidate.Transaction.Side)
	assert.Equal(t, "stripe", candidate.Transaction.Provider)
	assert.Equal(t, "tx_789", candidate.Transaction.ExternalID)
	assert.Equal(t, int64(2500), candidate.Transaction.Amount)
	assert.Equal(t, "GBP", candidate.Transaction.Currency)
	assert.Equal(t, 0.78, candidate.Score)
	assert.Len(t, candidate.Reasons, 2)
	assert.Contains(t, candidate.Reasons, "amount within 5%")
	assert.Contains(t, candidate.Reasons, "matching reference")
}

func TestMatchResult_JSONSerialization(t *testing.T) {
	matchID := uuid.New()
	policyID := uuid.New()

	result := MatchResult{
		Match: &models.Match{
			ID:       matchID,
			PolicyID: policyID,
			Score:    0.98,
			Decision: models.DecisionMatched,
		},
		Candidates: []Candidate{
			{
				Transaction: &models.Transaction{
					ID:         uuid.New(),
					PolicyID:   ptrUUIDMatcher(policyID),
					Side:       models.TransactionSideLedger,
					Provider:   "ledger",
					ExternalID: "ledger_001",
					Amount:     1000,
					Currency:   "USD",
					OccurredAt: time.Now().UTC().Truncate(time.Microsecond),
					IngestedAt: time.Now().UTC().Truncate(time.Microsecond),
				},
				Score:   0.98,
				Reasons: []string{"exact match"},
			},
		},
		Decision: DecisionMatched,
	}

	data, err := json.Marshal(result)
	require.NoError(t, err)

	var decoded MatchResult
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, result.Decision, decoded.Decision)
	assert.Equal(t, result.Match.ID, decoded.Match.ID)
	assert.Equal(t, result.Match.PolicyID, decoded.Match.PolicyID)
	assert.Len(t, decoded.Candidates, 1)
	assert.Equal(t, result.Candidates[0].Score, decoded.Candidates[0].Score)
	assert.Equal(t, result.Candidates[0].Reasons, decoded.Candidates[0].Reasons)
}

func TestMatchResult_JSONSerialization_NoMatch(t *testing.T) {
	result := MatchResult{
		Match: nil,
		Candidates: []Candidate{
			{
				Transaction: &models.Transaction{
					ID:         uuid.New(),
					PolicyID:   ptrUUIDMatcher(uuid.New()),
					Side:       models.TransactionSidePayments,
					Provider:   "stripe",
					ExternalID: "pay_001",
					Amount:     500,
					Currency:   "EUR",
					OccurredAt: time.Now().UTC(),
					IngestedAt: time.Now().UTC(),
				},
				Score:   0.45,
				Reasons: []string{"partial amount match"},
			},
		},
		Decision: DecisionRequiresReview,
	}

	data, err := json.Marshal(result)
	require.NoError(t, err)

	var decoded MatchResult
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Nil(t, decoded.Match)
	assert.Equal(t, DecisionRequiresReview, decoded.Decision)
	assert.Len(t, decoded.Candidates, 1)
}

func TestMatchResult_JSONSerialization_EmptyCandidates(t *testing.T) {
	result := MatchResult{
		Match:      nil,
		Candidates: []Candidate{},
		Decision:   DecisionUnmatched,
	}

	data, err := json.Marshal(result)
	require.NoError(t, err)

	var decoded MatchResult
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Nil(t, decoded.Match)
	assert.Equal(t, DecisionUnmatched, decoded.Decision)
	assert.Empty(t, decoded.Candidates)
}

func TestMatchResult_JSONDeserialization(t *testing.T) {
	matchID := uuid.New()
	policyID := uuid.New()
	txID := uuid.New()

	jsonData := `{
		"match": {
			"id": "` + matchID.String() + `",
			"policyID": "` + policyID.String() + `",
			"score": 0.92,
			"decision": "MATCHED",
			"createdAt": "2024-01-15T12:00:00Z"
		},
		"candidates": [
			{
				"transaction": {
					"id": "` + txID.String() + `",
					"policyID": "` + policyID.String() + `",
					"side": "PAYMENTS",
					"provider": "adyen",
					"externalID": "adyen_123",
					"amount": 1500,
					"currency": "USD",
					"occurredAt": "2024-01-15T11:00:00Z",
					"ingestedAt": "2024-01-15T11:01:00Z"
				},
				"score": 0.92,
				"reasons": ["amount match", "reference match"]
			}
		],
		"decision": "MATCHED"
	}`

	var result MatchResult
	err := json.Unmarshal([]byte(jsonData), &result)
	require.NoError(t, err)

	assert.Equal(t, matchID, result.Match.ID)
	assert.Equal(t, policyID, result.Match.PolicyID)
	assert.Equal(t, DecisionMatched, result.Decision)
	assert.Len(t, result.Candidates, 1)
	assert.Equal(t, txID, result.Candidates[0].Transaction.ID)
	assert.Equal(t, int64(1500), result.Candidates[0].Transaction.Amount)
	assert.Equal(t, 0.92, result.Candidates[0].Score)
}
