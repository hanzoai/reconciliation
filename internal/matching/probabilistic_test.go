package matching

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/elasticsearch"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptrUUIDProb(u uuid.UUID) *uuid.UUID {
	return &u
}

func TestProbabilisticMatcher_GetSearchQuery_NoPolicyIDFilter(t *testing.T) {
	// Test that policy_id is NOT included in the ES query since it was removed from ES documents
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
		Mode: "transactional",
		ScoringConfig: &models.ScoringConfig{
			TimeWindowHours:        24,
			AmountTolerancePercent: 1.0,
		},
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUIDProb(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_123",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
	}

	query := matcher.GetSearchQuery(sourceTx)

	// Verify query structure
	queryObj, ok := query["query"].(map[string]interface{})
	require.True(t, ok, "expected query object")

	boolQuery, ok := queryObj["bool"].(map[string]interface{})
	require.True(t, ok, "expected bool query")

	mustClauses, ok := boolQuery["must"].([]map[string]interface{})
	require.True(t, ok, "expected must clauses")

	// Verify policy_id is NOT in the query (removed from ES)
	foundPolicyID := false
	for _, clause := range mustClauses {
		if term, ok := clause["term"].(map[string]interface{}); ok {
			if _, ok := term["policy_id"]; ok {
				foundPolicyID = true
			}
		}
	}
	assert.False(t, foundPolicyID, "policy_id should NOT be in query (removed from ES)")
}

func TestProbabilisticMatcher_GetSearchQuery_OppositeSideFilter(t *testing.T) {
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
		Mode: "transactional",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	tests := []struct {
		name         string
		sourceSide   models.TransactionSide
		expectedSide string
	}{
		{
			name:         "ledger source searches payments",
			sourceSide:   models.TransactionSideLedger,
			expectedSide: "PAYMENTS",
		},
		{
			name:         "payments source searches ledger",
			sourceSide:   models.TransactionSidePayments,
			expectedSide: "LEDGER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceTx := &models.Transaction{
				ID:         uuid.New(),
				PolicyID:   ptrUUIDProb(policyID),
				Side:       tt.sourceSide,
				Provider:   "test",
				ExternalID: "tx_123",
				Amount:     1000,
				Currency:   "USD",
				OccurredAt: time.Now(),
			}

			query := matcher.GetSearchQuery(sourceTx)

			queryObj := query["query"].(map[string]interface{})
			boolQuery := queryObj["bool"].(map[string]interface{})
			mustClauses := boolQuery["must"].([]map[string]interface{})

			// Find the side filter
			foundSide := false
			for _, clause := range mustClauses {
				if term, ok := clause["term"].(map[string]interface{}); ok {
					if sideVal, ok := term["side"].(string); ok {
						assert.Equal(t, tt.expectedSide, sideVal)
						foundSide = true
					}
				}
			}
			assert.True(t, foundSide, "expected side filter in query")
		})
	}
}

func TestProbabilisticMatcher_GetSearchQuery_TimeWindowRespected(t *testing.T) {
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
		Mode: "transactional",
		ScoringConfig: &models.ScoringConfig{
			TimeWindowHours: 48,
		},
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	now := time.Now().UTC()
	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUIDProb(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_123",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: now,
	}

	query := matcher.GetSearchQuery(sourceTx)

	queryObj := query["query"].(map[string]interface{})
	boolQuery := queryObj["bool"].(map[string]interface{})
	mustClauses := boolQuery["must"].([]map[string]interface{})

	// Find the time range filter
	foundRange := false
	for _, clause := range mustClauses {
		if rangeClause, ok := clause["range"].(map[string]interface{}); ok {
			if occurredAt, ok := rangeClause["occurred_at"].(map[string]interface{}); ok {
				foundRange = true
				// Verify gte and lte are present
				_, hasGte := occurredAt["gte"]
				_, hasLte := occurredAt["lte"]
				assert.True(t, hasGte, "expected gte in range query")
				assert.True(t, hasLte, "expected lte in range query")
			}
		}
	}
	assert.True(t, foundRange, "expected occurred_at range filter in query")
}

func TestProbabilisticMatcher_GetSearchQuery_CurrencyFilter(t *testing.T) {
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
		Mode: "transactional",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUIDProb(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_123",
		Amount:     1000,
		Currency:   "EUR",
		OccurredAt: time.Now(),
	}

	query := matcher.GetSearchQuery(sourceTx)

	queryObj := query["query"].(map[string]interface{})
	boolQuery := queryObj["bool"].(map[string]interface{})
	mustClauses := boolQuery["must"].([]map[string]interface{})

	// Find the currency filter
	foundCurrency := false
	for _, clause := range mustClauses {
		if term, ok := clause["term"].(map[string]interface{}); ok {
			if currencyVal, ok := term["currency"].(string); ok {
				assert.Equal(t, "EUR", currencyVal)
				foundCurrency = true
			}
		}
	}
	assert.True(t, foundCurrency, "expected currency filter in query")
}

func TestProbabilisticMatcher_Match_NilTransaction(t *testing.T) {
	policy := &models.Policy{
		ID:   uuid.New(),
		Name: "test-policy",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	result, err := matcher.Match(context.Background(), nil)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "transaction cannot be nil")
}

func TestProbabilisticMatcher_CalculateAmountScore(t *testing.T) {
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
		Mode: "transactional",
		ScoringConfig: &models.ScoringConfig{
			AmountTolerancePercent: 1.0,
		},
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())
	config := matcher.getScoringConfig()

	tests := []struct {
		name           string
		sourceAmount   int64
		candidateAmt   int64
		expectedScore  float64
		toleranceRange float64
	}{
		{
			name:           "exact match",
			sourceAmount:   1000,
			candidateAmt:   1000,
			expectedScore:  1.0,
			toleranceRange: 0.01,
		},
		{
			name:           "within tolerance - 0.5% diff",
			sourceAmount:   1000,
			candidateAmt:   995,
			expectedScore:  0.75, // Approximately 0.75 for 0.5% diff
			toleranceRange: 0.1,
		},
		{
			name:           "at tolerance boundary",
			sourceAmount:   1000,
			candidateAmt:   990,
			expectedScore:  0.5, // At 1% diff with 1% tolerance
			toleranceRange: 0.1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &models.Transaction{Amount: tt.sourceAmount}
			candidate := &models.Transaction{Amount: tt.candidateAmt}

			score := matcher.calculateAmountScore(source, candidate, config)

			assert.InDelta(t, tt.expectedScore, score, tt.toleranceRange,
				"expected score around %.2f, got %.2f", tt.expectedScore, score)
		})
	}
}

func TestProbabilisticMatcher_CalculateDateScore(t *testing.T) {
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
		Mode: "transactional",
		ScoringConfig: &models.ScoringConfig{
			TimeWindowHours: 24,
		},
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())
	config := matcher.getScoringConfig()

	now := time.Now().UTC()

	// Uses Gaussian decay: score = 2^(-(delta/window)²)
	// At delta = window, score = 0.5
	// At delta = window/2, score = 2^(-0.25) ≈ 0.84
	tests := []struct {
		name          string
		sourceTime    time.Time
		candidateTime time.Time
		expectedScore float64
		tolerance     float64
	}{
		{
			name:          "same time",
			sourceTime:    now,
			candidateTime: now,
			expectedScore: 1.0,
			tolerance:     0.01,
		},
		{
			name:          "12 hours difference (half window)",
			sourceTime:    now,
			candidateTime: now.Add(12 * time.Hour),
			expectedScore: 0.84, // 2^(-0.25) ≈ 0.8409
			tolerance:     0.02,
		},
		{
			name:          "24 hours difference (at window)",
			sourceTime:    now,
			candidateTime: now.Add(24 * time.Hour),
			expectedScore: 0.5, // 2^(-1) = 0.5
			tolerance:     0.01,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &models.Transaction{OccurredAt: tt.sourceTime}
			candidate := &models.Transaction{OccurredAt: tt.candidateTime}

			score := matcher.calculateDateScore(source, candidate, config)

			assert.InDelta(t, tt.expectedScore, score, tt.tolerance,
				"expected score %.2f, got %.2f", tt.expectedScore, score)
		})
	}
}

func TestProbabilisticMatcher_CalculateMetadataScore(t *testing.T) {
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	tests := []struct {
		name           string
		sourceMetadata map[string]interface{}
		candidateMeta  map[string]interface{}
		expectedScore  float64
	}{
		{
			name: "all fields match",
			sourceMetadata: map[string]interface{}{
				"order_id": "order-123",
				"status":   "completed",
			},
			candidateMeta: map[string]interface{}{
				"order_id": "order-123",
				"status":   "completed",
			},
			expectedScore: 1.0,
		},
		{
			name: "half fields match",
			sourceMetadata: map[string]interface{}{
				"order_id": "order-123",
				"status":   "completed",
			},
			candidateMeta: map[string]interface{}{
				"order_id": "order-123",
				"status":   "pending",
			},
			expectedScore: 0.5,
		},
		{
			name: "no fields match",
			sourceMetadata: map[string]interface{}{
				"order_id": "order-123",
			},
			candidateMeta: map[string]interface{}{
				"order_id": "order-456",
			},
			expectedScore: 0.0,
		},
		{
			name:           "empty source metadata",
			sourceMetadata: map[string]interface{}{},
			candidateMeta: map[string]interface{}{
				"order_id": "order-123",
			},
			expectedScore: 0.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := &models.Transaction{Metadata: tt.sourceMetadata}
			candidate := &models.Transaction{Metadata: tt.candidateMeta}

			score := matcher.calculateMetadataScore(source, candidate, nil)

			assert.Equal(t, tt.expectedScore, score)
		})
	}
}

func TestProbabilisticMatcher_CalculateMetadataScore_Config(t *testing.T) {
	policy := &models.Policy{
		ID:   uuid.New(),
		Name: "test-policy",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	source := &models.Transaction{
		Metadata: map[string]interface{}{
			"order_id": "ORDER-123",
			"status":   "Completed",
			"extra":    "ignored",
		},
	}
	candidate := &models.Transaction{
		Metadata: map[string]interface{}{
			"order_id": "order-123",
			"status":   "completed",
			"extra":    "ignored",
		},
	}

	score := matcher.calculateMetadataScore(source, candidate, &models.ScoringConfig{
		MetadataFields:          []string{"order_id", "status"},
		MetadataCaseInsensitive: true,
	})

	assert.Equal(t, 1.0, score)
}

func TestProbabilisticMatcher_BuildResult_Thresholds(t *testing.T) {
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
		Mode: "transactional",
		ScoringConfig: &models.ScoringConfig{
			Thresholds: &models.ScoringThresholds{
				AutoMatch: 0.9,
				Review:    0.7,
			},
		},
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())
	config := matcher.getScoringConfig()

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUIDProb(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_123",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
	}

	candidateTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUIDProb(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "tx_456",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
	}

	tests := []struct {
		name             string
		candidateScore   float64
		expectedDecision Decision
		expectMatch      bool
	}{
		{
			name:             "above auto-match threshold",
			candidateScore:   0.95,
			expectedDecision: DecisionMatched,
			expectMatch:      true,
		},
		{
			name:             "at auto-match threshold",
			candidateScore:   0.9,
			expectedDecision: DecisionMatched,
			expectMatch:      true,
		},
		{
			name:             "between review and auto-match",
			candidateScore:   0.8,
			expectedDecision: DecisionRequiresReview,
			expectMatch:      false,
		},
		{
			name:             "at review threshold",
			candidateScore:   0.7,
			expectedDecision: DecisionRequiresReview,
			expectMatch:      false,
		},
		{
			name:             "below review threshold",
			candidateScore:   0.6,
			expectedDecision: DecisionUnmatched,
			expectMatch:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			candidates := []Candidate{
				{
					Transaction: candidateTx,
					Score:       tt.candidateScore,
					Reasons:     []string{"test reason"},
				},
			}

			result, err := matcher.buildResult(sourceTx, candidates, config)
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Equal(t, tt.expectedDecision, result.Decision)
			if tt.expectMatch {
				assert.NotNil(t, result.Match)
				assert.Equal(t, tt.candidateScore, result.Match.Score)
			} else {
				assert.Nil(t, result.Match)
			}
		})
	}
}

func TestProbabilisticMatcher_DefaultConfig(t *testing.T) {
	config := DefaultProbabilisticMatcherConfig()

	assert.Equal(t, 10, config.MaxCandidates)
}

func TestProbabilisticMatcher_CreateMatch_LedgerSource(t *testing.T) {
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	sourceTxID := uuid.New()
	candidateTxID := uuid.New()

	sourceTx := &models.Transaction{
		ID:       sourceTxID,
		PolicyID: ptrUUIDProb(policyID),
		Side:     models.TransactionSideLedger,
	}

	candidate := Candidate{
		Transaction: &models.Transaction{
			ID:       candidateTxID,
			PolicyID: ptrUUIDProb(policyID),
			Side:     models.TransactionSidePayments,
		},
		Score:   0.95,
		Reasons: []string{"test reason"},
	}

	match := matcher.createMatch(sourceTx, candidate)

	assert.NotNil(t, match)
	require.NotNil(t, match.PolicyID)
	assert.Equal(t, policyID, *match.PolicyID)
	assert.Contains(t, match.LedgerTransactionIDs, sourceTxID)
	assert.Contains(t, match.PaymentsTransactionIDs, candidateTxID)
	assert.Equal(t, 0.95, match.Score)
	assert.Equal(t, models.DecisionMatched, match.Decision)
	assert.Equal(t, "Probabilistic match found", match.Explanation.Reason)
	assert.Equal(t, "probabilistic", match.Explanation.FieldNotes["match_type"])
}

func TestProbabilisticMatcher_CreateMatch_PaymentsSource(t *testing.T) {
	policyID := uuid.New()
	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	sourceTxID := uuid.New()
	candidateTxID := uuid.New()

	sourceTx := &models.Transaction{
		ID:       sourceTxID,
		PolicyID: ptrUUIDProb(policyID),
		Side:     models.TransactionSidePayments,
	}

	candidate := Candidate{
		Transaction: &models.Transaction{
			ID:       candidateTxID,
			PolicyID: ptrUUIDProb(policyID),
			Side:     models.TransactionSideLedger,
		},
		Score:   0.92,
		Reasons: []string{"test reason"},
	}

	match := matcher.createMatch(sourceTx, candidate)

	assert.NotNil(t, match)
	// When source is Payments, candidate is Ledger
	assert.Contains(t, match.LedgerTransactionIDs, candidateTxID)
	assert.Contains(t, match.PaymentsTransactionIDs, sourceTxID)
}

func TestProbabilisticMatcher_BuildResult_EmptyCandidates(t *testing.T) {
	policy := &models.Policy{
		ID:   uuid.New(),
		Name: "test-policy",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())
	config := matcher.getScoringConfig()

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   &policy.ID,
		Side:       models.TransactionSideLedger,
		OccurredAt: time.Now(),
	}

	result, err := matcher.buildResult(sourceTx, []Candidate{}, config)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionUnmatched, result.Decision)
	assert.Nil(t, result.Match)
	assert.Empty(t, result.Candidates)
}

func TestProbabilisticMatcher_GetScoringConfig_Defaults(t *testing.T) {
	policy := &models.Policy{
		ID:            uuid.New(),
		Name:          "test-policy",
		ScoringConfig: nil, // No config
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())
	config := matcher.getScoringConfig()

	assert.Equal(t, 24, config.TimeWindowHours)
	assert.Equal(t, 1.0, config.AmountTolerancePercent)
	assert.NotNil(t, config.Weights)
	assert.Equal(t, 0.5, config.Weights.Amount)
	assert.Equal(t, 0.3, config.Weights.Date)
	assert.Equal(t, 0.2, config.Weights.Metadata)
	assert.NotNil(t, config.Thresholds)
	assert.Equal(t, 0.9, config.Thresholds.AutoMatch)
	assert.Equal(t, 0.7, config.Thresholds.Review)
}

func TestProbabilisticMatcher_GetScoringConfig_CustomConfig(t *testing.T) {
	policy := &models.Policy{
		ID:   uuid.New(),
		Name: "test-policy",
		ScoringConfig: &models.ScoringConfig{
			TimeWindowHours:        48,
			AmountTolerancePercent: 2.0,
			Weights: &models.ScoringWeights{
				Amount:   0.6,
				Date:     0.2,
				Metadata: 0.2,
			},
			Thresholds: &models.ScoringThresholds{
				AutoMatch: 0.85,
				Review:    0.65,
			},
		},
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())
	config := matcher.getScoringConfig()

	assert.Equal(t, 48, config.TimeWindowHours)
	assert.Equal(t, 2.0, config.AmountTolerancePercent)
	assert.Equal(t, 0.6, config.Weights.Amount)
	assert.Equal(t, 0.2, config.Weights.Date)
	assert.Equal(t, 0.2, config.Weights.Metadata)
	assert.Equal(t, 0.85, config.Thresholds.AutoMatch)
	assert.Equal(t, 0.65, config.Thresholds.Review)
}

func TestProbabilisticMatcher_NewWithZeroMaxCandidates(t *testing.T) {
	policy := &models.Policy{
		ID:   uuid.New(),
		Name: "test-policy",
	}

	// Zero MaxCandidates should be set to default
	matcher := NewProbabilisticMatcher(nil, policy, "", ProbabilisticMatcherConfig{
		MaxCandidates: 0,
	})

	assert.Equal(t, 10, matcher.config.MaxCandidates)
}

func TestProbabilisticMatcher_DocumentToTransaction(t *testing.T) {
	txID := uuid.New()
	now := time.Now().UTC().Truncate(time.Millisecond)

	policy := &models.Policy{
		ID:   uuid.New(),
		Name: "test-policy",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	doc := elasticsearch.TransactionDocument{
		TransactionID: txID.String(),
		Side:          "PAYMENTS",
		Provider:      "stripe",
		Amount:        5000,
		Currency:      "EUR",
		OccurredAt:    now,
		Metadata: map[string]interface{}{
			"order_id": "order-123",
		},
	}

	tx, err := matcher.documentToTransaction(doc)
	require.NoError(t, err)
	require.NotNil(t, tx)

	assert.Equal(t, txID, tx.ID)
	assert.Nil(t, tx.PolicyID) // PolicyID is no longer in the document
	assert.Equal(t, models.TransactionSidePayments, tx.Side)
	assert.Equal(t, "stripe", tx.Provider)
	assert.Equal(t, int64(5000), tx.Amount)
	assert.Equal(t, "EUR", tx.Currency)
	assert.Equal(t, now, tx.OccurredAt)
	assert.Equal(t, "order-123", tx.Metadata["order_id"])
}

func TestProbabilisticMatcher_DocumentToTransaction_InvalidUUIDs(t *testing.T) {
	policy := &models.Policy{
		ID:   uuid.New(),
		Name: "test-policy",
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())

	tests := []struct {
		name        string
		doc         elasticsearch.TransactionDocument
		expectError string
	}{
		{
			name: "invalid transaction_id",
			doc: elasticsearch.TransactionDocument{
				TransactionID: "invalid-uuid",
			},
			expectError: "invalid transaction_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx, err := matcher.documentToTransaction(tt.doc)
			require.Error(t, err)
			assert.Nil(t, tx)
			assert.Contains(t, err.Error(), tt.expectError)
		})
	}
}

func TestProbabilisticMatcher_CalculateScore_AllWeights(t *testing.T) {
	policyID := uuid.New()
	now := time.Now().UTC()

	policy := &models.Policy{
		ID:   policyID,
		Name: "test-policy",
		ScoringConfig: &models.ScoringConfig{
			TimeWindowHours:        24,
			AmountTolerancePercent: 1.0,
			Weights: &models.ScoringWeights{
				Amount:   0.5,
				Date:     0.3,
				Metadata: 0.2,
			},
		},
	}

	matcher := NewProbabilisticMatcher(nil, policy, "", DefaultProbabilisticMatcherConfig())
	config := matcher.getScoringConfig()

	source := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUIDProb(policyID),
		Amount:     1000,
		OccurredAt: now,
		Metadata: map[string]interface{}{
			"order_id": "order-123",
		},
	}

	candidate := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUIDProb(policyID),
		Amount:     1000,
		OccurredAt: now,
		Metadata: map[string]interface{}{
			"order_id": "order-123",
		},
	}

	score, reasons := matcher.calculateScore(source, candidate, config)

	// Perfect match should have score close to 1.0
	assert.InDelta(t, 1.0, score, 0.01)
	assert.NotEmpty(t, reasons)
	assert.Len(t, reasons, 3) // amount, date, metadata
}
