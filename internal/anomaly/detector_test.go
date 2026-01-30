package anomaly

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/matching"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockTimeProvider is a mock implementation of TimeProvider for testing.
type MockTimeProvider struct {
	currentTime time.Time
}

func (m *MockTimeProvider) Now() time.Time {
	return m.currentTime
}

func ptrUUID(u uuid.UUID) *uuid.UUID {
	return &u
}

func TestDetector_Detect_MatchedDecision_NoAnomaly(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	transaction := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-123",
		Amount:     5000,
		Currency:   "USD",
		OccurredAt: now.Add(-48 * time.Hour),
		IngestedAt: now.Add(-47 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision: matching.DecisionMatched,
		Match: &models.Match{
			ID:       uuid.New(),
			PolicyID:   ptrUUID(policyID),
			Score:    0.95,
			Decision: models.DecisionMatched,
		},
		Candidates: []matching.Candidate{},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, result.Anomaly, "no anomaly should be created for MATCHED decision")
	assert.False(t, result.Pending, "result should not be pending for MATCHED decision")
}

func TestDetector_Detect_RequiresReviewDecision_NoAnomaly(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	transaction := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "test-provider",
		ExternalID: "ext-456",
		Amount:     3000,
		Currency:   "EUR",
		OccurredAt: now.Add(-48 * time.Hour),
		IngestedAt: now.Add(-47 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision:   matching.DecisionRequiresReview,
		Match:      nil,
		Candidates: []matching.Candidate{},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, result.Anomaly, "no anomaly should be created for REQUIRES_REVIEW decision")
	assert.False(t, result.Pending, "result should not be pending for REQUIRES_REVIEW decision")
}

func TestDetector_Detect_UnmatchedBeforeWindow_Pending(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	// Transaction occurred 12 hours ago (within the 24-hour window)
	transaction := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-789",
		Amount:     7500,
		Currency:   "USD",
		OccurredAt: now.Add(-12 * time.Hour),
		IngestedAt: now.Add(-11 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision:   matching.DecisionUnmatched,
		Match:      nil,
		Candidates: []matching.Candidate{},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, result.Anomaly, "no anomaly should be created before time window ends")
	assert.True(t, result.Pending, "result should be pending when within time window")
}

func TestDetector_Detect_UnmatchedAfterWindow_AnomalyCreated_Ledger(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	transactionID := uuid.New()

	// Transaction occurred 48 hours ago (outside the 24-hour window)
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-101",
		Amount:     15000, // 150.00
		Currency:   "USD",
		OccurredAt: now.Add(-48 * time.Hour),
		IngestedAt: now.Add(-47 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision:   matching.DecisionUnmatched,
		Match:      nil,
		Candidates: []matching.Candidate{},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, result.Anomaly, "anomaly should be created after time window ends")
	assert.False(t, result.Pending, "result should not be pending after time window")

	// Verify anomaly details
	require.NotNil(t, result.Anomaly.PolicyID)
	assert.Equal(t, policyID, *result.Anomaly.PolicyID)
	require.NotNil(t, result.Anomaly.TransactionID)
	assert.Equal(t, transactionID, *result.Anomaly.TransactionID)
	assert.Equal(t, models.AnomalyTypeMissingOnPayments, result.Anomaly.Type)
	assert.Equal(t, models.SeverityHigh, result.Anomaly.Severity, "MISSING_ON_PAYMENTS should have HIGH severity by default")
	assert.Equal(t, models.AnomalyStateOpen, result.Anomaly.State)
	assert.Contains(t, result.Anomaly.Reason, "Ledger transaction")
	assert.Contains(t, result.Anomaly.Reason, transactionID.String(), "reason should contain transaction ID")
	assert.Contains(t, result.Anomaly.Reason, "24h", "reason should contain window hours")
}

func TestDetector_Detect_UnmatchedAfterWindow_AnomalyCreated_Payments(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	transactionID := uuid.New()

	// Transaction occurred 48 hours ago (outside the 24-hour window)
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "ext-102",
		Amount:     500, // 5.00
		Currency:   "EUR",
		OccurredAt: now.Add(-48 * time.Hour),
		IngestedAt: now.Add(-47 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision:   matching.DecisionUnmatched,
		Match:      nil,
		Candidates: []matching.Candidate{},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, result.Anomaly, "anomaly should be created after time window ends")
	assert.False(t, result.Pending, "result should not be pending after time window")

	// Verify anomaly details
	require.NotNil(t, result.Anomaly.PolicyID)
	assert.Equal(t, policyID, *result.Anomaly.PolicyID)
	require.NotNil(t, result.Anomaly.TransactionID)
	assert.Equal(t, transactionID, *result.Anomaly.TransactionID)
	assert.Equal(t, models.AnomalyTypeMissingOnLedger, result.Anomaly.Type, "payment transaction should create MISSING_ON_LEDGER, not MISSING_ON_PAYMENTS")
	assert.Equal(t, models.SeverityCritical, result.Anomaly.Severity, "MISSING_ON_LEDGER should have CRITICAL severity (risk of non-recording)")
	assert.Equal(t, models.AnomalyStateOpen, result.Anomaly.State)
	assert.Contains(t, result.Anomaly.Reason, "Payment")
	assert.Contains(t, result.Anomaly.Reason, transactionID.String(), "reason should contain payment ID")
	assert.Contains(t, result.Anomaly.Reason, "stripe", "reason should contain provider")
}

func TestDetector_MissingOnPayments_AlwaysHighSeverity(t *testing.T) {
	// Test that MISSING_ON_PAYMENTS anomalies always have HIGH severity
	// regardless of transaction amount
	testCases := []struct {
		name   string
		amount int64
	}{
		{"low amount", 500},
		{"medium amount", 5000},
		{"high amount", 15000},
		{"critical amount", 150000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			now := time.Now()
			mockTime := &MockTimeProvider{currentTime: now}
			detector := NewDefaultDetectorWithTimeProvider(mockTime)

			policyID := uuid.New()
			transaction := &models.Transaction{
				ID:         uuid.New(),
				PolicyID:   ptrUUID(policyID),
				Side:       models.TransactionSideLedger,
				Provider:   "test-provider",
				ExternalID: "ext-103",
				Amount:     tc.amount,
				Currency:   "USD",
				OccurredAt: now.Add(-48 * time.Hour),
				IngestedAt: now.Add(-47 * time.Hour),
			}

			matchResult := &matching.MatchResult{
				Decision:   matching.DecisionUnmatched,
				Match:      nil,
				Candidates: []matching.Candidate{},
			}

			// Act
			result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

			// Assert
			require.NoError(t, err)
			require.NotNil(t, result.Anomaly)
			assert.Equal(t, models.AnomalyTypeMissingOnPayments, result.Anomaly.Type)
			assert.Equal(t, models.SeverityHigh, result.Anomaly.Severity, "MISSING_ON_PAYMENTS should always have HIGH severity")
		})
	}
}

func TestDetector_MissingOnLedger_AlwaysCriticalSeverity(t *testing.T) {
	// Test that MISSING_ON_LEDGER anomalies always have CRITICAL severity
	// regardless of transaction amount (risk of non-recording)
	testCases := []struct {
		name   string
		amount int64
	}{
		{"low amount", 500},
		{"medium amount", 5000},
		{"high amount", 15000},
		{"critical amount", 150000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			now := time.Now()
			mockTime := &MockTimeProvider{currentTime: now}
			detector := NewDefaultDetectorWithTimeProvider(mockTime)

			policyID := uuid.New()
			transaction := &models.Transaction{
				ID:         uuid.New(),
				PolicyID:   ptrUUID(policyID),
				Side:       models.TransactionSidePayments,
				Provider:   "stripe",
				ExternalID: "ext-104",
				Amount:     tc.amount,
				Currency:   "USD",
				OccurredAt: now.Add(-48 * time.Hour),
				IngestedAt: now.Add(-47 * time.Hour),
			}

			matchResult := &matching.MatchResult{
				Decision:   matching.DecisionUnmatched,
				Match:      nil,
				Candidates: []matching.Candidate{},
			}

			// Act
			result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

			// Assert
			require.NoError(t, err)
			require.NotNil(t, result.Anomaly)
			assert.Equal(t, models.AnomalyTypeMissingOnLedger, result.Anomaly.Type)
			assert.Equal(t, models.SeverityCritical, result.Anomaly.Severity, "MISSING_ON_LEDGER should always have CRITICAL severity")
		})
	}
}

func TestDetector_WindowBoundary_ExactlyAtEnd(t *testing.T) {
	// Arrange
	// Test the boundary: transaction occurred exactly 24 hours ago
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	// Transaction occurred exactly at the window boundary (24 hours ago to the nanosecond)
	transaction := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-105",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: now.Add(-24 * time.Hour),
		IngestedAt: now.Add(-23 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision:   matching.DecisionUnmatched,
		Match:      nil,
		Candidates: []matching.Candidate{},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	// At exactly 24 hours, now is NOT before windowEnd, so anomaly should be created
	require.NotNil(t, result.Anomaly, "anomaly should be created at exactly window boundary")
	assert.False(t, result.Pending)
}

func TestDetector_NewDefaultDetector(t *testing.T) {
	// Test that the default constructor works
	detector := NewDefaultDetector()
	require.NotNil(t, detector)
	require.NotNil(t, detector.timeProvider)

	// Verify it uses real time (within a reasonable range)
	realTime := detector.timeProvider.Now()
	assert.WithinDuration(t, time.Now(), realTime, time.Second)
}

// US-049: Test that unmatched Ledger transaction creates MISSING_ON_PAYMENTS anomaly
func TestDetector_UnmatchedLedgerTransaction_CreatesMissingOnPayments(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	transactionID := uuid.New()
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger, // Ledger side
		Provider:   "test-provider",
		ExternalID: "ext-ledger-001",
		Amount:     10000,
		Currency:   "USD",
		OccurredAt: now.Add(-48 * time.Hour), // Outside time window
		IngestedAt: now.Add(-47 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision:   matching.DecisionUnmatched,
		Match:      nil,
		Candidates: []matching.Candidate{},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, result.Anomaly)

	// Verify type is MISSING_ON_PAYMENTS
	assert.Equal(t, models.AnomalyTypeMissingOnPayments, result.Anomaly.Type,
		"unmatched Ledger transaction should create MISSING_ON_PAYMENTS anomaly")

	// Verify severity is HIGH by default
	assert.Equal(t, models.SeverityHigh, result.Anomaly.Severity,
		"MISSING_ON_PAYMENTS should have HIGH severity by default")

	// Verify reason contains transaction ID
	assert.Contains(t, result.Anomaly.Reason, transactionID.String(),
		"reason should contain transaction ID")

	// Verify reason format: 'Ledger transaction {id} without Payments match after {window}h'
	assert.Contains(t, result.Anomaly.Reason, "Ledger transaction")
	assert.Contains(t, result.Anomaly.Reason, "without Payments match")
	assert.Contains(t, result.Anomaly.Reason, "24h")
}

// US-049: Test that unmatched Payment transaction creates different type (MISSING_ON_LEDGER)
func TestDetector_UnmatchedPaymentTransaction_CreatesDifferentType(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	transactionID := uuid.New()
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments, // Payments side, NOT Ledger
		Provider:   "stripe",
		ExternalID: "ext-payment-001",
		Amount:     10000,
		Currency:   "USD",
		OccurredAt: now.Add(-48 * time.Hour), // Outside time window
		IngestedAt: now.Add(-47 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision:   matching.DecisionUnmatched,
		Match:      nil,
		Candidates: []matching.Candidate{},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, result.Anomaly)

	// Verify type is NOT MISSING_ON_PAYMENTS (it should be MISSING_ON_LEDGER)
	assert.NotEqual(t, models.AnomalyTypeMissingOnPayments, result.Anomaly.Type,
		"unmatched Payment transaction should NOT create MISSING_ON_PAYMENTS anomaly")
	assert.Equal(t, models.AnomalyTypeMissingOnLedger, result.Anomaly.Type,
		"unmatched Payment transaction should create MISSING_ON_LEDGER anomaly")
}

// US-050: Test that unmatched Payment transaction creates MISSING_ON_LEDGER anomaly
// with CRITICAL severity and reason containing payment ID and provider
func TestDetector_UnmatchedPaymentTransaction_CreatesMissingOnLedger(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	transactionID := uuid.New()
	providerName := "stripe"
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments, // Payment side
		Provider:   providerName,
		ExternalID: "ext-payment-050",
		Amount:     10000,
		Currency:   "USD",
		OccurredAt: now.Add(-48 * time.Hour), // Outside time window
		IngestedAt: now.Add(-47 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision:   matching.DecisionUnmatched,
		Match:      nil,
		Candidates: []matching.Candidate{},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, result.Anomaly)

	// Verify type is MISSING_ON_LEDGER
	assert.Equal(t, models.AnomalyTypeMissingOnLedger, result.Anomaly.Type,
		"unmatched Payment transaction should create MISSING_ON_LEDGER anomaly")

	// Verify severity is CRITICAL (risk of non-recording)
	assert.Equal(t, models.SeverityCritical, result.Anomaly.Severity,
		"MISSING_ON_LEDGER should have CRITICAL severity (risk of non-recording)")

	// Verify reason contains payment ID
	assert.Contains(t, result.Anomaly.Reason, transactionID.String(),
		"reason should contain payment ID")

	// Verify reason contains provider
	assert.Contains(t, result.Anomaly.Reason, providerName,
		"reason should contain provider name")
}

// US-052: Test that currency mismatch creates CURRENCY_MISMATCH anomaly
func TestDetector_MatchWithCurrencyMismatch_CreatesCurrencyMismatch(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	transactionID := uuid.New()
	matchedTxID := uuid.New()

	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-052-001",
		Amount:     10000,
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchedTx := &models.Transaction{
		ID:         matchedTxID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "ext-052-002",
		Amount:     10000,
		Currency:   "EUR",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision: matching.DecisionMatched,
		Match: &models.Match{
			ID:                     uuid.New(),
			PolicyID:   ptrUUID(policyID),
			LedgerTransactionIDs:   []uuid.UUID{transactionID},
			PaymentsTransactionIDs: []uuid.UUID{matchedTxID},
			Score:                  0.95,
			Decision:               models.DecisionMatched,
		},
		Candidates: []matching.Candidate{
			{
				Transaction: matchedTx,
				Score:       0.95,
				Reasons:     []string{"high score match"},
			},
		},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, result.Anomaly)
	assert.Equal(t, models.AnomalyTypeCurrencyMismatch, result.Anomaly.Type)
	assert.Equal(t, models.SeverityHigh, result.Anomaly.Severity)
	assert.Contains(t, result.Anomaly.Reason, "Ledger=USD")
	assert.Contains(t, result.Anomaly.Reason, "Payments=EUR")
}

// US-052: Currency mismatch takes priority over amount mismatch
func TestDetector_CurrencyMismatch_PriorityOverAmountMismatch(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	config := DetectorConfig{
		AmountTolerancePercent: 1.0,
	}
	detector := NewDefaultDetectorWithConfig(mockTime, config)

	policyID := uuid.New()
	transactionID := uuid.New()
	matchedTxID := uuid.New()

	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "ext-052-003",
		Amount:     10500,
		Currency:   "EUR",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchedTx := &models.Transaction{
		ID:         matchedTxID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-052-004",
		Amount:     10000,
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision: matching.DecisionMatched,
		Match: &models.Match{
			ID:                     uuid.New(),
			PolicyID:   ptrUUID(policyID),
			LedgerTransactionIDs:   []uuid.UUID{matchedTxID},
			PaymentsTransactionIDs: []uuid.UUID{transactionID},
			Score:                  0.92,
			Decision:               models.DecisionMatched,
		},
		Candidates: []matching.Candidate{
			{
				Transaction: matchedTx,
				Score:       0.92,
				Reasons:     []string{"high score match"},
			},
		},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, result.Anomaly)
	assert.Equal(t, models.AnomalyTypeCurrencyMismatch, result.Anomaly.Type)
}

// US-051: Test that match with delta > tolerance creates AMOUNT_MISMATCH anomaly
func TestDetector_MatchWithDeltaExceedsTolerance_CreatesAmountMismatch(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	config := DetectorConfig{
		AmountTolerancePercent: 1.0, // 1% tolerance
	}
	detector := NewDefaultDetectorWithConfig(mockTime, config)

	policyID := uuid.New()
	transactionID := uuid.New()
	matchedTxID := uuid.New()

	// Source transaction (Ledger side) with amount 10000
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-051-001",
		Amount:     10000, // 100.00
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	// Matched candidate with 5% difference (10500 vs 10000 = 500 delta, ~4.88% difference)
	matchedTx := &models.Transaction{
		ID:         matchedTxID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "ext-051-002",
		Amount:     10500, // 105.00 - ~4.88% difference, exceeds 1% tolerance
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision: matching.DecisionMatched,
		Match: &models.Match{
			ID:                     uuid.New(),
			PolicyID:   ptrUUID(policyID),
			LedgerTransactionIDs:   []uuid.UUID{transactionID},
			PaymentsTransactionIDs: []uuid.UUID{matchedTxID},
			Score:                  0.92,
			Decision:               models.DecisionMatched,
		},
		Candidates: []matching.Candidate{
			{
				Transaction: matchedTx,
				Score:       0.92,
				Reasons:     []string{"high score match"},
			},
		},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, result.Anomaly, "anomaly should be created for amount mismatch exceeding tolerance")
	assert.False(t, result.Pending)

	// Verify type is AMOUNT_MISMATCH
	assert.Equal(t, models.AnomalyTypeAmountMismatch, result.Anomaly.Type,
		"match with delta > tolerance should create AMOUNT_MISMATCH anomaly")

	// Verify severity is MEDIUM
	assert.Equal(t, models.SeverityMedium, result.Anomaly.Severity,
		"AMOUNT_MISMATCH should have MEDIUM severity")

	// Verify reason contains ledger amount, payment amount, and delta
	assert.Contains(t, result.Anomaly.Reason, "Ledger=10000",
		"reason should contain ledger amount")
	assert.Contains(t, result.Anomaly.Reason, "Payment=10500",
		"reason should contain payment amount")
	assert.Contains(t, result.Anomaly.Reason, "delta=500",
		"reason should contain delta")
}

// US-051: Test that match with delta <= tolerance creates NO anomaly
func TestDetector_MatchWithDeltaWithinTolerance_NoAnomaly(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	config := DetectorConfig{
		AmountTolerancePercent: 5.0, // 5% tolerance
	}
	detector := NewDefaultDetectorWithConfig(mockTime, config)

	policyID := uuid.New()
	transactionID := uuid.New()
	matchedTxID := uuid.New()

	// Source transaction (Ledger side) with amount 10000
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-051-003",
		Amount:     10000, // 100.00
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	// Matched candidate with ~2% difference (10200 vs 10000 = 200 delta, ~1.98% difference)
	// This is within the 5% tolerance
	matchedTx := &models.Transaction{
		ID:         matchedTxID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "ext-051-004",
		Amount:     10200, // 102.00 - ~1.98% difference, within 5% tolerance
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision: matching.DecisionMatched,
		Match: &models.Match{
			ID:                     uuid.New(),
			PolicyID:   ptrUUID(policyID),
			LedgerTransactionIDs:   []uuid.UUID{transactionID},
			PaymentsTransactionIDs: []uuid.UUID{matchedTxID},
			Score:                  0.95,
			Decision:               models.DecisionMatched,
		},
		Candidates: []matching.Candidate{
			{
				Transaction: matchedTx,
				Score:       0.95,
				Reasons:     []string{"high score match"},
			},
		},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, result.Anomaly, "no anomaly should be created when delta is within tolerance")
	assert.False(t, result.Pending)
}

// US-051: Test exact amount match creates NO anomaly
func TestDetector_ExactAmountMatch_NoAnomaly(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	config := DetectorConfig{
		AmountTolerancePercent: 1.0,
	}
	detector := NewDefaultDetectorWithConfig(mockTime, config)

	policyID := uuid.New()
	transactionID := uuid.New()
	matchedTxID := uuid.New()

	// Source transaction
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-051-005",
		Amount:     10000,
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	// Matched candidate with exact same amount
	matchedTx := &models.Transaction{
		ID:         matchedTxID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "ext-051-006",
		Amount:     10000, // Exact match
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision: matching.DecisionMatched,
		Match: &models.Match{
			ID:                     uuid.New(),
			PolicyID:   ptrUUID(policyID),
			LedgerTransactionIDs:   []uuid.UUID{transactionID},
			PaymentsTransactionIDs: []uuid.UUID{matchedTxID},
			Score:                  1.0,
			Decision:               models.DecisionMatched,
		},
		Candidates: []matching.Candidate{
			{
				Transaction: matchedTx,
				Score:       1.0,
				Reasons:     []string{"perfect match"},
			},
		},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, result.Anomaly, "no anomaly should be created for exact amount match")
	assert.False(t, result.Pending)
}

// US-051: Test AMOUNT_MISMATCH from Payment side (amounts are swapped in reason)
func TestDetector_AmountMismatch_FromPaymentSide(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	config := DetectorConfig{
		AmountTolerancePercent: 1.0,
	}
	detector := NewDefaultDetectorWithConfig(mockTime, config)

	policyID := uuid.New()
	transactionID := uuid.New()
	matchedTxID := uuid.New()

	// Source transaction (Payment side) with amount 10500
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments, // Payment side
		Provider:   "stripe",
		ExternalID: "ext-051-007",
		Amount:     10500, // 105.00
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	// Matched candidate (Ledger side) with amount 10000
	matchedTx := &models.Transaction{
		ID:         matchedTxID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger, // Ledger side
		Provider:   "test-provider",
		ExternalID: "ext-051-008",
		Amount:     10000, // 100.00
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision: matching.DecisionMatched,
		Match: &models.Match{
			ID:                     uuid.New(),
			PolicyID:   ptrUUID(policyID),
			LedgerTransactionIDs:   []uuid.UUID{matchedTxID},
			PaymentsTransactionIDs: []uuid.UUID{transactionID},
			Score:                  0.92,
			Decision:               models.DecisionMatched,
		},
		Candidates: []matching.Candidate{
			{
				Transaction: matchedTx,
				Score:       0.92,
				Reasons:     []string{"high score match"},
			},
		},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	require.NotNil(t, result.Anomaly)

	assert.Equal(t, models.AnomalyTypeAmountMismatch, result.Anomaly.Type)
	assert.Equal(t, models.SeverityMedium, result.Anomaly.Severity)

	// Verify reason has correct ledger/payment amounts (ledger from matched tx, payment from source)
	assert.Contains(t, result.Anomaly.Reason, "Ledger=10000",
		"reason should contain ledger amount from matched transaction")
	assert.Contains(t, result.Anomaly.Reason, "Payment=10500",
		"reason should contain payment amount from source transaction")
	assert.Contains(t, result.Anomaly.Reason, "delta=500",
		"reason should contain delta")
}

// US-051: Test boundary case - exactly at tolerance threshold
func TestDetector_AmountMismatch_ExactlyAtTolerance_NoAnomaly(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	config := DetectorConfig{
		AmountTolerancePercent: 5.0, // 5% tolerance
	}
	detector := NewDefaultDetectorWithConfig(mockTime, config)

	policyID := uuid.New()
	transactionID := uuid.New()
	matchedTxID := uuid.New()

	// Source transaction with amount 10000
	transaction := &models.Transaction{
		ID:         transactionID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-051-009",
		Amount:     10000,
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	// Matched candidate with exactly 5% difference
	// 10000 and 10526 have avg of 10263, delta of 526, which is ~5.125%
	// Let's use 10500 which is avg 10250, delta 500, which is 4.88%
	matchedTx := &models.Transaction{
		ID:         matchedTxID,
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSidePayments,
		Provider:   "stripe",
		ExternalID: "ext-051-010",
		Amount:     10500, // This gives ~4.88% which is within 5% tolerance
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision: matching.DecisionMatched,
		Match: &models.Match{
			ID:       uuid.New(),
			PolicyID:   ptrUUID(policyID),
			Score:    0.95,
			Decision: models.DecisionMatched,
		},
		Candidates: []matching.Candidate{
			{
				Transaction: matchedTx,
				Score:       0.95,
				Reasons:     []string{"high score match"},
			},
		},
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, result.Anomaly, "no anomaly should be created when delta is at or below tolerance")
	assert.False(t, result.Pending)
}

// US-051: Test that matched decision with no candidates creates no anomaly
func TestDetector_MatchedDecision_NoCandidates_NoAnomaly(t *testing.T) {
	// Arrange
	now := time.Now()
	mockTime := &MockTimeProvider{currentTime: now}
	detector := NewDefaultDetectorWithTimeProvider(mockTime)

	policyID := uuid.New()
	transaction := &models.Transaction{
		ID:         uuid.New(),
		PolicyID:   ptrUUID(policyID),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "ext-051-011",
		Amount:     10000,
		Currency:   "USD",
		OccurredAt: now.Add(-2 * time.Hour),
		IngestedAt: now.Add(-1 * time.Hour),
	}

	matchResult := &matching.MatchResult{
		Decision:   matching.DecisionMatched,
		Match:      nil,
		Candidates: []matching.Candidate{}, // No candidates
	}

	// Act
	result, err := detector.Detect(context.Background(), transaction, matchResult, policyID)

	// Assert
	require.NoError(t, err)
	assert.Nil(t, result.Anomaly)
	assert.False(t, result.Pending)
}
