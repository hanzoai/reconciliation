package anomaly

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/formancehq/reconciliation/internal/matching"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
)

// DetectResult represents the result of anomaly detection.
type DetectResult struct {
	Anomaly *models.Anomaly
	Pending bool // true if still within time window, anomaly not yet created
}

// Detector defines the interface for detecting anomalies in transactions.
type Detector interface {
	Detect(ctx context.Context, transaction *models.Transaction, matchResult *matching.MatchResult, policyID uuid.UUID) (*DetectResult, error)
}

// TimeProvider is an interface for getting the current time, allowing for testing.
type TimeProvider interface {
	Now() time.Time
}

// RealTimeProvider returns the actual current time.
type RealTimeProvider struct{}

func (r *RealTimeProvider) Now() time.Time {
	return time.Now()
}

// DetectorConfig holds configuration for the anomaly detector.
type DetectorConfig struct {
	// AmountTolerancePercent is the allowed percentage difference in amounts.
	// If the amount delta exceeds this tolerance, an AMOUNT_MISMATCH anomaly is created.
	AmountTolerancePercent float64
}

// DefaultDetectorConfig returns the default configuration.
func DefaultDetectorConfig() DetectorConfig {
	return DetectorConfig{
		AmountTolerancePercent: 1.0, // Default 1% tolerance
	}
}

// DefaultDetector is the default implementation of the Detector interface.
type DefaultDetector struct {
	timeProvider TimeProvider
	config       DetectorConfig
}

// NewDefaultDetector creates a new DefaultDetector with the real time provider.
func NewDefaultDetector() *DefaultDetector {
	return &DefaultDetector{
		timeProvider: &RealTimeProvider{},
		config:       DefaultDetectorConfig(),
	}
}

// NewDefaultDetectorWithTimeProvider creates a new DefaultDetector with a custom time provider.
// This is useful for testing.
func NewDefaultDetectorWithTimeProvider(tp TimeProvider) *DefaultDetector {
	return &DefaultDetector{
		timeProvider: tp,
		config:       DefaultDetectorConfig(),
	}
}

// NewDefaultDetectorWithConfig creates a new DefaultDetector with a custom time provider and config.
// This is useful for testing.
func NewDefaultDetectorWithConfig(tp TimeProvider, config DetectorConfig) *DefaultDetector {
	return &DefaultDetector{
		timeProvider: tp,
		config:       config,
	}
}

// Detect checks if an anomaly should be created for a transaction based on its match result.
// It creates an anomaly if:
// 1. The match result decision is UNMATCHED and the time window has passed
// 2. The match result decision is MATCHED but the amount delta exceeds tolerance (AMOUNT_MISMATCH)
//
// If the decision is REQUIRES_REVIEW, no anomaly is created.
// If the decision is UNMATCHED but the time window hasn't ended yet, the result is pending.
func (d *DefaultDetector) Detect(
	ctx context.Context,
	transaction *models.Transaction,
	matchResult *matching.MatchResult,
	policyID uuid.UUID,
) (*DetectResult, error) {
	// No anomaly for REQUIRES_REVIEW decisions
	if matchResult.Decision == matching.DecisionRequiresReview {
		return &DetectResult{
			Anomaly: nil,
			Pending: false,
		}, nil
	}

	// For MATCHED decisions, check for amount mismatch
	if matchResult.Decision == matching.DecisionMatched {
		return d.detectAmountMismatch(transaction, matchResult, policyID)
	}

	// For UNMATCHED decisions, check if time window has ended
	// Default time window is 24 hours if not specified
	timeWindowHours := 24

	// Calculate the window end time
	windowEnd := transaction.OccurredAt.Add(time.Duration(timeWindowHours) * time.Hour)
	now := d.timeProvider.Now()

	// If we're still within the time window, mark as pending
	if now.Before(windowEnd) {
		return &DetectResult{
			Anomaly: nil,
			Pending: true,
		}, nil
	}

	// Time window has ended and no match found - create anomaly
	anomaly := d.createAnomaly(transaction, timeWindowHours, policyID)

	return &DetectResult{
		Anomaly: anomaly,
		Pending: false,
	}, nil
}

// createAnomaly creates an Anomaly for an unmatched transaction.
func (d *DefaultDetector) createAnomaly(transaction *models.Transaction, windowHours int, policyID uuid.UUID) *models.Anomaly {
	anomalyType := d.determineAnomalyType(transaction)
	severity := d.determineSeverity(transaction, anomalyType)

	return &models.Anomaly{
		ID:            uuid.New(),
		PolicyID:      policyID,
		TransactionID: transaction.ID,
		Type:          anomalyType,
		Severity:      severity,
		State:         models.AnomalyStateOpen,
		Reason:        d.buildReason(transaction, anomalyType, windowHours),
		CreatedAt:     d.timeProvider.Now(),
	}
}

// determineAnomalyType determines the type of anomaly based on the transaction.
func (d *DefaultDetector) determineAnomalyType(transaction *models.Transaction) models.AnomalyType {
	// If the transaction is from Ledger but no matching Payment was found
	if transaction.Side == models.TransactionSideLedger {
		return models.AnomalyTypeMissingOnPayments
	}
	// If the transaction is from Payments but no matching Ledger entry was found
	return models.AnomalyTypeMissingOnLedger
}

// determineSeverity determines the severity of an anomaly based on the transaction and type.
func (d *DefaultDetector) determineSeverity(transaction *models.Transaction, anomalyType models.AnomalyType) models.Severity {
	// MISSING_ON_LEDGER is CRITICAL severity (risk of non-recording)
	if anomalyType == models.AnomalyTypeMissingOnLedger {
		return models.SeverityCritical
	}
	// MISSING_ON_PAYMENTS defaults to HIGH severity
	if anomalyType == models.AnomalyTypeMissingOnPayments {
		return models.SeverityHigh
	}

	// For other anomaly types, use amount-based severity
	if transaction.Amount > 100000 { // 1000.00 in cents
		return models.SeverityCritical
	}
	if transaction.Amount > 10000 { // 100.00 in cents
		return models.SeverityHigh
	}
	if transaction.Amount > 1000 { // 10.00 in cents
		return models.SeverityMedium
	}
	return models.SeverityLow
}

// buildReason builds a human-readable reason for the anomaly.
func (d *DefaultDetector) buildReason(transaction *models.Transaction, anomalyType models.AnomalyType, windowHours int) string {
	switch anomalyType {
	case models.AnomalyTypeMissingOnPayments:
		return fmt.Sprintf("Ledger transaction %s without Payments match after %dh", transaction.ID.String(), windowHours)
	case models.AnomalyTypeMissingOnLedger:
		return fmt.Sprintf("Payment %s from provider %s without Ledger match after %dh", transaction.ID.String(), transaction.Provider, windowHours)
	default:
		return fmt.Sprintf("Unmatched transaction %s after %dh", transaction.ID.String(), windowHours)
	}
}

// detectAmountMismatch checks if a matched transaction has an amount discrepancy beyond tolerance.
// Returns an AMOUNT_MISMATCH anomaly if the delta exceeds the configured tolerance.
func (d *DefaultDetector) detectAmountMismatch(
	transaction *models.Transaction,
	matchResult *matching.MatchResult,
	policyID uuid.UUID,
) (*DetectResult, error) {
	// Need candidates to compare amounts
	if len(matchResult.Candidates) == 0 {
		return &DetectResult{
			Anomaly: nil,
			Pending: false,
		}, nil
	}

	// Get the matched candidate (first one with the highest score)
	matchedCandidate := matchResult.Candidates[0]
	if matchedCandidate.Transaction == nil {
		return &DetectResult{
			Anomaly: nil,
			Pending: false,
		}, nil
	}

	// Calculate amount delta
	sourceAmount := transaction.Amount
	matchedAmount := matchedCandidate.Transaction.Amount
	delta := int64(math.Abs(float64(sourceAmount - matchedAmount)))

	// If amounts are equal, no anomaly
	if delta == 0 {
		return &DetectResult{
			Anomaly: nil,
			Pending: false,
		}, nil
	}

	// Calculate percentage difference
	avgAmount := float64(sourceAmount+matchedAmount) / 2.0
	if avgAmount == 0 {
		return &DetectResult{
			Anomaly: nil,
			Pending: false,
		}, nil
	}
	percentDiff := (float64(delta) / avgAmount) * 100

	// Check if delta exceeds tolerance
	if percentDiff <= d.config.AmountTolerancePercent {
		return &DetectResult{
			Anomaly: nil,
			Pending: false,
		}, nil
	}

	// Delta exceeds tolerance - create AMOUNT_MISMATCH anomaly
	// Determine ledger and payment amounts based on transaction side
	var ledgerAmount, paymentAmount int64
	if transaction.Side == models.TransactionSideLedger {
		ledgerAmount = sourceAmount
		paymentAmount = matchedAmount
	} else {
		ledgerAmount = matchedAmount
		paymentAmount = sourceAmount
	}

	anomaly := &models.Anomaly{
		ID:            uuid.New(),
		PolicyID:      policyID,
		TransactionID: transaction.ID,
		Type:          models.AnomalyTypeAmountMismatch,
		Severity:      models.SeverityMedium,
		State:         models.AnomalyStateOpen,
		Reason: fmt.Sprintf(
			"Amount mismatch: Ledger=%d, Payment=%d, delta=%d (%.2f%%)",
			ledgerAmount, paymentAmount, delta, percentDiff,
		),
		CreatedAt: d.timeProvider.Now(),
	}

	return &DetectResult{
		Anomaly: anomaly,
		Pending: false,
	}, nil
}
