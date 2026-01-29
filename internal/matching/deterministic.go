package matching

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

// ErrMultipleMatches is returned when multiple matches are found and strict mode is enabled.
var ErrMultipleMatches = errors.New("multiple deterministic matches found")

// DeterministicMatcherConfig holds configuration for the DeterministicMatcher.
type DeterministicMatcherConfig struct {
	// StrictMode when true returns an error if multiple matches are found.
	// When false, returns the first match.
	StrictMode bool
}

// DeterministicMatcher implements matching based on shared IDs (deterministic fields).
// It uses OpenSearch (via TransactionStore) for all searches, providing scalable
// matching on any field (direct or metadata).
type DeterministicMatcher struct {
	txStore storage.TransactionStore
	policy  *models.Policy
	config  DeterministicMatcherConfig
}

// NewDeterministicMatcher creates a new DeterministicMatcher instance.
func NewDeterministicMatcher(
	txStore storage.TransactionStore,
	policy *models.Policy,
	config DeterministicMatcherConfig,
) *DeterministicMatcher {
	return &DeterministicMatcher{
		txStore: txStore,
		policy:  policy,
		config:  config,
	}
}

// Match attempts to find a deterministic match for the given transaction.
// It searches for transactions on the opposite side that share the same value
// for any of the configured deterministic fields.
func (m *DeterministicMatcher) Match(ctx context.Context, transaction *models.Transaction) (*MatchResult, error) {
	if transaction == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	// Determine the opposite side to search
	oppositeSide := getOppositeSide(transaction.Side)

	// Get the deterministic fields to search by
	fields := m.getDeterministicFields()

	// Collect all matching candidates
	var candidates []Candidate

	for _, field := range fields {
		fieldValue := m.getFieldValue(transaction, field)
		if fieldValue == "" {
			continue
		}

		// Search for matching transactions on the opposite side using OpenSearch
		matches, err := m.findMatchesByField(ctx, oppositeSide, field, fieldValue)
		if err != nil {
			return nil, fmt.Errorf("failed to search by field %s: %w", field, err)
		}

		for _, match := range matches {
			candidates = append(candidates, Candidate{
				Transaction: match,
				Score:       1.0,
				Reasons:     []string{fmt.Sprintf("exact match on %s", field)},
			})
		}
	}

	// Remove duplicates (same transaction matched on multiple fields)
	candidates = m.deduplicateCandidates(candidates)

	// Handle the result based on configuration
	return m.buildResult(transaction, candidates)
}

// getDeterministicFields returns the fields to use for deterministic matching.
// If the policy has no deterministic fields configured, defaults to external_id.
func (m *DeterministicMatcher) getDeterministicFields() []string {
	if len(m.policy.DeterministicFields) == 0 {
		return []string{"external_id"}
	}
	return m.policy.DeterministicFields
}

// getFieldValue extracts the value of a field from a transaction.
// Supports both direct transaction fields and metadata fields.
func (m *DeterministicMatcher) getFieldValue(tx *models.Transaction, field string) string {
	switch field {
	case "external_id":
		return tx.ExternalID
	case "provider":
		return tx.Provider
	case "currency":
		return tx.Currency
	default:
		// Check metadata for the field
		if tx.Metadata != nil {
			if val, ok := tx.Metadata[field]; ok {
				return fmt.Sprintf("%v", val)
			}
		}
		return ""
	}
}

// findMatchesByField searches for transactions matching a specific field value.
// Uses OpenSearch via TransactionStore for all field types.
func (m *DeterministicMatcher) findMatchesByField(
	ctx context.Context,
	side models.TransactionSide,
	field string,
	value string,
) ([]*models.Transaction, error) {
	// Use TransactionStore.SearchByField for all fields
	// This works for both direct fields (external_id, provider, currency)
	// and metadata fields (order_id, user_id, etc.)
	matches, err := m.txStore.SearchByField(ctx, side, field, value)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return matches, nil
}

// deduplicateCandidates removes duplicate candidates (same transaction ID).
// When duplicates are found, reasons are merged.
func (m *DeterministicMatcher) deduplicateCandidates(candidates []Candidate) []Candidate {
	seen := make(map[uuid.UUID]*Candidate)
	var result []Candidate

	for _, c := range candidates {
		if existing, ok := seen[c.Transaction.ID]; ok {
			// Merge reasons
			existing.Reasons = append(existing.Reasons, c.Reasons...)
		} else {
			// Copy the candidate to avoid reference issues
			candidate := Candidate{
				Transaction: c.Transaction,
				Score:       c.Score,
				Reasons:     make([]string, len(c.Reasons)),
			}
			copy(candidate.Reasons, c.Reasons)
			seen[c.Transaction.ID] = &candidate
			result = append(result, candidate)
		}
	}

	// Update merged reasons in result
	for i := range result {
		if merged, ok := seen[result[i].Transaction.ID]; ok {
			result[i].Reasons = merged.Reasons
		}
	}

	return result
}

// buildResult constructs the MatchResult based on candidates and configuration.
func (m *DeterministicMatcher) buildResult(
	sourceTx *models.Transaction,
	candidates []Candidate,
) (*MatchResult, error) {
	if len(candidates) == 0 {
		return &MatchResult{
			Match:      nil,
			Candidates: []Candidate{},
			Decision:   DecisionUnmatched,
		}, nil
	}

	// Handle based on topology
	switch m.policy.Topology {
	case "1:N":
		return m.build1ToNResult(sourceTx, candidates)
	case "N:1":
		return m.buildNTo1Result(sourceTx, candidates)
	default:
		// Default 1:1 behavior
		return m.build1To1Result(sourceTx, candidates)
	}
}

// build1To1Result handles standard 1:1 matching (one source to one candidate).
func (m *DeterministicMatcher) build1To1Result(
	sourceTx *models.Transaction,
	candidates []Candidate,
) (*MatchResult, error) {
	// Handle multiple matches in strict mode
	if len(candidates) > 1 && m.config.StrictMode {
		return nil, ErrMultipleMatches
	}

	// Use the first candidate as the match
	matchedTx := candidates[0].Transaction

	// Determine which transaction is ledger and which is payments
	var ledgerTxIDs, paymentTxIDs []uuid.UUID
	if sourceTx.Side == models.TransactionSideLedger {
		ledgerTxIDs = []uuid.UUID{sourceTx.ID}
		paymentTxIDs = []uuid.UUID{matchedTx.ID}
	} else {
		ledgerTxIDs = []uuid.UUID{matchedTx.ID}
		paymentTxIDs = []uuid.UUID{sourceTx.ID}
	}

	match := &models.Match{
		ID:                     uuid.New(),
		PolicyID:               m.policy.ID,
		LedgerTransactionIDs:   ledgerTxIDs,
		PaymentsTransactionIDs: paymentTxIDs,
		Score:                  1.0,
		Decision:               models.DecisionMatched,
		Explanation: models.Explanation{
			Reason:     "Deterministic match found",
			Score:      1.0,
			FieldNotes: map[string]string{"match_type": "deterministic"},
		},
	}

	return &MatchResult{
		Match:      match,
		Candidates: candidates,
		Decision:   DecisionMatched,
	}, nil
}

// build1ToNResult handles 1:N matching (one Ledger to multiple Payments).
// The source must be a Ledger transaction and candidates must be Payments.
// The sum of Payment amounts must match the Ledger amount within tolerance.
func (m *DeterministicMatcher) build1ToNResult(
	sourceTx *models.Transaction,
	candidates []Candidate,
) (*MatchResult, error) {
	// 1:N topology requires source to be Ledger and candidates to be Payments
	if sourceTx.Side != models.TransactionSideLedger {
		// If source is from Payments side, fall back to 1:1
		return m.build1To1Result(sourceTx, candidates)
	}

	// Collect all payment transaction IDs and calculate sum
	var paymentTxIDs []uuid.UUID
	var totalPaymentAmount int64

	for _, c := range candidates {
		paymentTxIDs = append(paymentTxIDs, c.Transaction.ID)
		totalPaymentAmount += c.Transaction.Amount
	}

	// Validate amount tolerance
	ledgerAmount := sourceTx.Amount
	tolerance := m.getAmountTolerancePercent()

	if !isWithinTolerance(ledgerAmount, totalPaymentAmount, tolerance) {
		// Amount mismatch - return unmatched
		return &MatchResult{
			Match:      nil,
			Candidates: candidates,
			Decision:   DecisionUnmatched,
		}, nil
	}

	match := &models.Match{
		ID:                     uuid.New(),
		PolicyID:               m.policy.ID,
		LedgerTransactionIDs:   []uuid.UUID{sourceTx.ID},
		PaymentsTransactionIDs: paymentTxIDs,
		Score:                  1.0,
		Decision:               models.DecisionMatched,
		Explanation: models.Explanation{
			Reason: "Deterministic 1:N match found",
			Score:  1.0,
			FieldNotes: map[string]string{
				"match_type":     "deterministic",
				"topology":       "1:N",
				"payment_count":  fmt.Sprintf("%d", len(candidates)),
				"ledger_amount":  fmt.Sprintf("%d", ledgerAmount),
				"payments_total": fmt.Sprintf("%d", totalPaymentAmount),
				"tolerance_pct":  fmt.Sprintf("%.2f", tolerance),
			},
		},
	}

	return &MatchResult{
		Match:      match,
		Candidates: candidates,
		Decision:   DecisionMatched,
	}, nil
}

// buildNTo1Result handles N:1 matching (multiple Ledgers to one Payment).
// The source must be a Payment transaction and candidates must be Ledgers.
// The sum of Ledger amounts must match the Payment amount within tolerance.
func (m *DeterministicMatcher) buildNTo1Result(
	sourceTx *models.Transaction,
	candidates []Candidate,
) (*MatchResult, error) {
	// N:1 topology requires source to be Payments and candidates to be Ledgers
	if sourceTx.Side != models.TransactionSidePayments {
		// If source is from Ledger side, fall back to 1:1
		return m.build1To1Result(sourceTx, candidates)
	}

	// Collect all ledger transaction IDs and calculate sum
	var ledgerTxIDs []uuid.UUID
	var totalLedgerAmount int64

	for _, c := range candidates {
		ledgerTxIDs = append(ledgerTxIDs, c.Transaction.ID)
		totalLedgerAmount += c.Transaction.Amount
	}

	// Validate amount tolerance
	paymentAmount := sourceTx.Amount
	tolerance := m.getAmountTolerancePercent()

	if !isWithinTolerance(paymentAmount, totalLedgerAmount, tolerance) {
		// Amount mismatch - return unmatched
		return &MatchResult{
			Match:      nil,
			Candidates: candidates,
			Decision:   DecisionUnmatched,
		}, nil
	}

	match := &models.Match{
		ID:                     uuid.New(),
		PolicyID:               m.policy.ID,
		LedgerTransactionIDs:   ledgerTxIDs,
		PaymentsTransactionIDs: []uuid.UUID{sourceTx.ID},
		Score:                  1.0,
		Decision:               models.DecisionMatched,
		Explanation: models.Explanation{
			Reason: "Deterministic N:1 match found",
			Score:  1.0,
			FieldNotes: map[string]string{
				"match_type":     "deterministic",
				"topology":       "N:1",
				"ledger_count":   fmt.Sprintf("%d", len(candidates)),
				"ledger_total":   fmt.Sprintf("%d", totalLedgerAmount),
				"payment_amount": fmt.Sprintf("%d", paymentAmount),
				"tolerance_pct":  fmt.Sprintf("%.2f", tolerance),
			},
		},
	}

	return &MatchResult{
		Match:      match,
		Candidates: candidates,
		Decision:   DecisionMatched,
	}, nil
}

// getAmountTolerancePercent returns the configured tolerance percentage.
// Returns 0 if no tolerance is configured (exact match required).
func (m *DeterministicMatcher) getAmountTolerancePercent() float64 {
	if m.policy.ScoringConfig != nil {
		return m.policy.ScoringConfig.AmountTolerancePercent
	}
	return 0
}

// isWithinTolerance checks if the actual amount is within tolerance of the expected amount.
func isWithinTolerance(expected, actual int64, tolerancePercent float64) bool {
	if tolerancePercent == 0 {
		return expected == actual
	}

	diff := expected - actual
	if diff < 0 {
		diff = -diff
	}

	// Calculate allowed difference
	allowedDiff := float64(expected) * (tolerancePercent / 100.0)

	return float64(diff) <= allowedDiff
}

// getOppositeSide returns the opposite transaction side.
func getOppositeSide(side models.TransactionSide) models.TransactionSide {
	if side == models.TransactionSideLedger {
		return models.TransactionSidePayments
	}
	return models.TransactionSideLedger
}
