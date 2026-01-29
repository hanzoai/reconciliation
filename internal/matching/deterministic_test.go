package matching

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func ptrUUID(u uuid.UUID) *uuid.UUID {
	return &u
}

func TestDeterministicMatcher_Match_ExternalID_Found(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"external_id"},
	}

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_123",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}

	matchedTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "tx_123",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}

	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "external_id", "tx_123").
		Return([]*models.Transaction{matchedTx}, nil)

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionMatched, result.Decision)
	assert.Equal(t, 1.0, result.Match.Score)
	assert.Len(t, result.Candidates, 1)
	assert.Equal(t, 1.0, result.Candidates[0].Score)
	assert.Contains(t, result.Candidates[0].Reasons, "exact match on external_id")
}

func TestDeterministicMatcher_Match_PaymentID_InMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"payment_id"},
	}

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "ledger_tx_001",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_abc123",
		},
	}

	matchedTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "stripe_tx_001",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_abc123",
		},
	}

	// SearchByField is called for metadata field search
	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "payment_id", "pay_abc123").
		Return([]*models.Transaction{matchedTx}, nil)

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionMatched, result.Decision)
	assert.Equal(t, 1.0, result.Match.Score)
	assert.Len(t, result.Candidates, 1)
	assert.Equal(t, 1.0, result.Candidates[0].Score)
	assert.Contains(t, result.Candidates[0].Reasons, "exact match on payment_id")
}

func TestDeterministicMatcher_Match_NoMatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"external_id"},
	}

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_not_found",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}

	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "external_id", "tx_not_found").
		Return(nil, storage.ErrNotFound)

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionUnmatched, result.Decision)
	assert.Nil(t, result.Match)
	assert.Empty(t, result.Candidates)
}

func TestDeterministicMatcher_Match_MultipleMatches_ReturnsFirst(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"payment_id"},
	}

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "ledger_tx_001",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_duplicate",
		},
	}

	matchedTx1 := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "stripe_tx_001",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_duplicate",
		},
	}

	matchedTx2 := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "adyen",
		ExternalID: "adyen_tx_001",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_duplicate",
		},
	}

	// Multiple matches returned
	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "payment_id", "pay_duplicate").
		Return([]*models.Transaction{matchedTx1, matchedTx2}, nil)

	// StrictMode = false, should return first match
	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{StrictMode: false})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionMatched, result.Decision)
	assert.Equal(t, 1.0, result.Match.Score)
	assert.Len(t, result.Candidates, 2)
	// First match should be used
	assert.Equal(t, matchedTx1.ID, result.Candidates[0].Transaction.ID)
}

func TestDeterministicMatcher_Match_MultipleMatches_StrictMode_ReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"payment_id"},
	}

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "ledger_tx_001",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_duplicate",
		},
	}

	matchedTx1 := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "stripe_tx_001",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_duplicate",
		},
	}

	matchedTx2 := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "adyen",
		ExternalID: "adyen_tx_001",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_duplicate",
		},
	}

	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "payment_id", "pay_duplicate").
		Return([]*models.Transaction{matchedTx1, matchedTx2}, nil)

	// StrictMode = true, should return error
	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{StrictMode: true})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMultipleMatches)
	assert.Nil(t, result)
}

func TestDeterministicMatcher_Match_EmptyDeterministicFields_UsesExternalID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	// Policy with empty deterministic fields - should default to external_id
	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: nil, // Empty/nil
	}

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_default_field",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}

	matchedTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "tx_default_field",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}

	// Expect search by external_id (the default)
	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "external_id", "tx_default_field").
		Return([]*models.Transaction{matchedTx}, nil)

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionMatched, result.Decision)
	assert.Equal(t, 1.0, result.Match.Score)
	assert.Contains(t, result.Candidates[0].Reasons, "exact match on external_id")
}

func TestDeterministicMatcher_Match_PaymentsSideSource(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"external_id"},
	}

	// Source transaction is from PAYMENTS side
	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "tx_from_payments",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}

	matchedTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_from_payments",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}

	// Should search on LEDGER side (opposite of PAYMENTS)
	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSideLedger, "external_id", "tx_from_payments").
		Return([]*models.Transaction{matchedTx}, nil)

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionMatched, result.Decision)
	// Verify correct transaction ID assignment
	assert.Contains(t, result.Match.LedgerTransactionIDs, matchedTx.ID)
	assert.Contains(t, result.Match.PaymentsTransactionIDs, sourceTx.ID)
}

func TestDeterministicMatcher_Match_NilTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policy := &models.Policy{
		ID:   uuid.New(),
		Name: "test-policy",
	}

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	result, err := matcher.Match(context.Background(), nil)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "transaction cannot be nil")
}

func TestDeterministicMatcher_Match_MultipleFields_MatchOnFirst(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"external_id", "payment_id"},
	}

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_multi_field",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_no_match",
		},
	}

	matchedTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "tx_multi_field",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}

	// First field (external_id) finds a match
	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "external_id", "tx_multi_field").
		Return([]*models.Transaction{matchedTx}, nil)

	// Second field (payment_id) finds no match
	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "payment_id", "pay_no_match").
		Return(nil, storage.ErrNotFound)

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionMatched, result.Decision)
	assert.Equal(t, 1.0, result.Match.Score)
	assert.Len(t, result.Candidates, 1)
}

func TestDeterministicMatcher_Match_EmptyMetadataValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"payment_id"},
	}

	// Source transaction has no payment_id in metadata
	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_no_metadata",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata:   nil,
	}

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	// No calls expected since field value is empty
	result, err := matcher.Match(context.Background(), sourceTx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionUnmatched, result.Decision)
	assert.Nil(t, result.Match)
	assert.Empty(t, result.Candidates)
}

func TestDeterministicMatcher_Deduplication(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()
	matchedTxID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"external_id", "payment_id"},
	}

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_dedup",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_dedup",
		},
	}

	matchedTx := &models.Transaction{
		ID:         matchedTxID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "tx_dedup",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata: map[string]interface{}{
			"payment_id": "pay_dedup",
		},
	}

	// Same transaction matched by external_id
	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "external_id", "tx_dedup").
		Return([]*models.Transaction{matchedTx}, nil)

	// Same transaction also matched by payment_id
	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "payment_id", "pay_dedup").
		Return([]*models.Transaction{matchedTx}, nil)

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, DecisionMatched, result.Decision)
	// Should only have 1 candidate after deduplication
	assert.Len(t, result.Candidates, 1)
	assert.Equal(t, matchedTxID, result.Candidates[0].Transaction.ID)
	// Reasons should be merged
	assert.Len(t, result.Candidates[0].Reasons, 2)
	assert.Contains(t, result.Candidates[0].Reasons, "exact match on external_id")
	assert.Contains(t, result.Candidates[0].Reasons, "exact match on payment_id")
}

func TestGetOppositeSide(t *testing.T) {
	tests := []struct {
		input    models.TransactionSide
		expected models.TransactionSide
	}{
		{models.TransactionSideLedger, models.TransactionSidePayments},
		{models.TransactionSidePayments, models.TransactionSideLedger},
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			result := getOppositeSide(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeterministicMatcher_Match_StoreError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)
	policyID := uuid.New()

	policy := &models.Policy{
		ID:                  policyID,
		Name:                "test-policy",
		DeterministicFields: []string{"external_id"},
	}

	sourceTx := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "ledger",
		ExternalID: "tx_error",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
	}

	mockStore.EXPECT().
		SearchByField(gomock.Any(), models.TransactionSidePayments, "external_id", "tx_error").
		Return(nil, assert.AnError)

	matcher := NewDeterministicMatcher(mockStore, policy, DeterministicMatcherConfig{})

	result, err := matcher.Match(context.Background(), sourceTx)
	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to search by field external_id")
}
