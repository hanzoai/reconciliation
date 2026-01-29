package service

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/anomaly"
	"github.com/formancehq/reconciliation/internal/matching"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// MatchResultV2 represents the result of matching a single transaction.
type MatchResultV2 struct {
	TransactionID string           `json:"transactionId"`
	Decision      string           `json:"decision"`
	Match         *MatchV2         `json:"match,omitempty"`
	Candidates    []MatchCandidate `json:"candidates,omitempty"`
	Error         string           `json:"error,omitempty"`
}

// MatchV2 represents a match in v2 API format.
type MatchV2 struct {
	ID                     uuid.UUID   `json:"id"`
	PolicyID               uuid.UUID   `json:"policyId"`
	LedgerTransactionIDs   []uuid.UUID `json:"ledgerTransactionIds"`
	PaymentsTransactionIDs []uuid.UUID `json:"paymentsTransactionIds"`
	Score                  float64     `json:"score"`
	Decision               string      `json:"decision"`
	CreatedAt              time.Time   `json:"createdAt"`
}

// MatchCandidate represents a match candidate for API response.
type MatchCandidate struct {
	TransactionID uuid.UUID `json:"transactionId"`
	Score         float64   `json:"score"`
	Explanation   string    `json:"explanation,omitempty"`
}

type ForceMatchRequest struct {
	LedgerTxIDs  []string `json:"ledger_tx_ids"`
	PaymentTxIDs []string `json:"payment_tx_ids"`
	Reason       string   `json:"reason"`
}

func (r *ForceMatchRequest) Validate() error {
	if r.Reason == "" {
		return errors.Wrap(ErrValidation, "missing reason")
	}

	if len(r.LedgerTxIDs) == 0 && len(r.PaymentTxIDs) == 0 {
		return errors.Wrap(ErrValidation, "at least one ledger_tx_ids or payment_tx_ids is required")
	}

	return nil
}

func (s *Service) ForceMatch(ctx context.Context, policyIDStr string, req *ForceMatchRequest, resolvedBy string) (*models.Match, error) {
	if err := req.Validate(); err != nil {
		return nil, err
	}

	policyID, err := uuid.Parse(policyIDStr)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, "invalid policyId")
	}

	// Verify the policy exists
	if _, err := s.store.GetPolicy(ctx, policyID); err != nil {
		return nil, newStorageError(err, "policy not found")
	}

	// Parse and validate ledger transaction IDs
	ledgerTxIDs := make([]uuid.UUID, 0, len(req.LedgerTxIDs))
	for _, idStr := range req.LedgerTxIDs {
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, errors.Wrap(ErrInvalidID, fmt.Sprintf("invalid ledger_tx_ids: %s", idStr))
		}
		ledgerTxIDs = append(ledgerTxIDs, id)
	}

	// Parse and validate payment transaction IDs
	paymentTxIDs := make([]uuid.UUID, 0, len(req.PaymentTxIDs))
	for _, idStr := range req.PaymentTxIDs {
		id, err := uuid.Parse(idStr)
		if err != nil {
			return nil, errors.Wrap(ErrInvalidID, fmt.Sprintf("invalid payment_tx_ids: %s", idStr))
		}
		paymentTxIDs = append(paymentTxIDs, id)
	}

	allTxIDs := make([]uuid.UUID, 0, len(ledgerTxIDs)+len(paymentTxIDs))
	allTxIDs = append(allTxIDs, ledgerTxIDs...)
	allTxIDs = append(allTxIDs, paymentTxIDs...)

	// Verify all transaction IDs exist in the transaction store
	for _, txID := range allTxIDs {
		if _, err := s.txStore.GetByID(ctx, txID); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, errors.Wrap(ErrInvalidID, fmt.Sprintf("transaction not found: %s", txID))
			}
			return nil, newStorageError(err, fmt.Sprintf("verifying transaction %s", txID))
		}
	}

	// Create the match with MANUAL_MATCH decision
	match := &models.Match{
		ID:                     uuid.New(),
		PolicyID:               policyID,
		LedgerTransactionIDs:   ledgerTxIDs,
		PaymentsTransactionIDs: paymentTxIDs,
		Score:                  1.0, // Manual match has perfect score
		Decision:               models.DecisionManualMatch,
		Explanation: models.Explanation{
			Reason: fmt.Sprintf("manual match: %s", req.Reason),
		},
		CreatedAt: time.Now().UTC(),
	}

	// Create the match in the database
	if err := s.store.CreateMatch(ctx, match); err != nil {
		return nil, newStorageError(err, "creating match")
	}

	// Find and resolve related anomalies
	anomalies, err := s.store.FindOpenAnomaliesByTransactionIDs(ctx, allTxIDs)
	if err != nil {
		return nil, newStorageError(err, "finding related anomalies")
	}

	for _, anom := range anomalies {
		if err := s.store.ResolveAnomaly(ctx, anom.ID, resolvedBy); err != nil {
			return nil, newStorageError(err, fmt.Sprintf("resolving anomaly %s", anom.ID.String()))
		}
	}

	return match, nil
}

// MatchTransactions runs matching for a list of transactions using the specified policy.
// This implementation uses deterministic matching based on the policy's deterministic fields.
// Transactions are retrieved from OpenSearch via TransactionStore.
func (s *Service) MatchTransactions(ctx context.Context, policyIDStr string, transactionIDs []string) ([]MatchResultV2, error) {
	// Parse and validate policy ID
	policyID, err := uuid.Parse(policyIDStr)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, "invalid policyId")
	}

	// Get the policy
	policy, err := s.store.GetPolicy(ctx, policyID)
	if err != nil {
		return nil, newStorageError(err, "policy not found")
	}

	// Verify policy is transactional
	if !policy.IsTransactional() {
		return nil, errors.Wrap(ErrValidation, "policy must be in transactional mode for matching")
	}

	// Create anomaly detector for orphan detection
	anomalyDetector := anomaly.NewDefaultDetector()

	// Process each transaction
	results := make([]MatchResultV2, 0, len(transactionIDs))
	for _, txIDStr := range transactionIDs {
		result := MatchResultV2{
			TransactionID: txIDStr,
		}

		// Parse transaction ID
		txID, err := uuid.Parse(txIDStr)
		if err != nil {
			result.Error = fmt.Sprintf("invalid transaction ID: %v", err)
			result.Decision = "ERROR"
			results = append(results, result)
			continue
		}

		// Get the transaction from OpenSearch
		tx, err := s.txStore.GetByID(ctx, txID)
		if err != nil {
			result.Error = fmt.Sprintf("transaction not found: %v", err)
			result.Decision = "ERROR"
			results = append(results, result)
			continue
		}

		// Run deterministic matching
		matchResult, err := s.runDeterministicMatching(ctx, policy, tx)
		if err != nil {
			result.Error = fmt.Sprintf("matching failed: %v", err)
			result.Decision = "ERROR"
			results = append(results, result)
			continue
		}

		// Convert result
		result.Decision = string(matchResult.Decision)

		if matchResult.Match != nil {
			result.Match = &MatchV2{
				ID:                     matchResult.Match.ID,
				PolicyID:               matchResult.Match.PolicyID,
				LedgerTransactionIDs:   matchResult.Match.LedgerTransactionIDs,
				PaymentsTransactionIDs: matchResult.Match.PaymentsTransactionIDs,
				Score:                  matchResult.Match.Score,
				Decision:               string(matchResult.Match.Decision),
				CreatedAt:              matchResult.Match.CreatedAt,
			}
		}

		// Detect and persist anomalies for unmatched transactions
		if matchResult.Decision == matching.DecisionUnmatched {
			detectResult, err := anomalyDetector.Detect(ctx, tx, matchResult, policyID)
			if err != nil {
				result.Error = fmt.Sprintf("anomaly detection failed: %v", err)
				results = append(results, result)
				continue
			}
			// If an anomaly was detected (not pending), persist it
			if detectResult != nil && detectResult.Anomaly != nil {
				logging.FromContext(ctx).WithFields(map[string]interface{}{
					"transaction_id": tx.ID,
					"anomaly_type":   detectResult.Anomaly.Type,
					"occurred_at":    tx.OccurredAt,
				}).Debug("Creating anomaly for unmatched transaction")
				if err := s.store.CreateAnomaly(ctx, detectResult.Anomaly); err != nil {
					result.Error = fmt.Sprintf("failed to persist anomaly: %v", err)
					results = append(results, result)
					continue
				}
			} else if detectResult != nil && detectResult.Pending {
				logging.FromContext(ctx).WithFields(map[string]interface{}{
					"transaction_id": tx.ID,
					"occurred_at":    tx.OccurredAt,
				}).Debug("Anomaly pending for transaction (still in time window)")
			}
		}

		results = append(results, result)
	}

	return results, nil
}

// TriggerPolicyMatching triggers matching for all transactions of a policy.
func (s *Service) TriggerPolicyMatching(ctx context.Context, policyIDStr string) ([]MatchResultV2, error) {
	policyID, err := uuid.Parse(policyIDStr)
	if err != nil {
		return nil, errors.Wrap(ErrInvalidID, "invalid policyId")
	}

	policy, err := s.store.GetPolicy(ctx, policyID)
	if err != nil {
		return nil, newStorageError(err, "policy not found")
	}

	if !policy.IsTransactional() {
		return nil, errors.Wrap(ErrValidation, "policy must be in transactional mode for matching")
	}

	transactionIDs, err := s.collectPolicyTransactionIDs(ctx, policy)
	if err != nil {
		return nil, err
	}

	if len(transactionIDs) == 0 {
		return []MatchResultV2{}, nil
	}

	return s.MatchTransactions(ctx, policyIDStr, transactionIDs)
}

// collectPolicyTransactionIDs collects all transaction IDs for a policy from OpenSearch.
func (s *Service) collectPolicyTransactionIDs(ctx context.Context, policy *models.Policy) ([]string, error) {
	var transactionIDs []string

	if policy.LedgerName != "" {
		ledgerTxs, err := s.txStore.GetByProvider(ctx, policy.LedgerName, models.TransactionSideLedger)
		if err != nil {
			return nil, fmt.Errorf("failed to get ledger transactions: %w", err)
		}
		for _, tx := range ledgerTxs {
			transactionIDs = append(transactionIDs, tx.ID.String())
		}
	}

	paymentProvider := policy.PaymentsProvider
	if policy.ConnectorType != nil && *policy.ConnectorType != "" {
		paymentProvider = *policy.ConnectorType
	}
	if paymentProvider != "" {
		paymentTxs, err := s.txStore.GetByProvider(ctx, paymentProvider, models.TransactionSidePayments)
		if err != nil {
			return nil, fmt.Errorf("failed to get payment transactions: %w", err)
		}
		for _, tx := range paymentTxs {
			transactionIDs = append(transactionIDs, tx.ID.String())
		}
	}

	return transactionIDs, nil
}

// runDeterministicMatching performs deterministic matching for a transaction.
// It uses TransactionStore (OpenSearch) to search for matches by external_id or configured deterministic fields.
func (s *Service) runDeterministicMatching(ctx context.Context, policy *models.Policy, tx *models.Transaction) (*matching.MatchResult, error) {
	// Determine the opposite side to search
	var oppositeSide models.TransactionSide
	if tx.Side == models.TransactionSideLedger {
		oppositeSide = models.TransactionSidePayments
	} else {
		oppositeSide = models.TransactionSideLedger
	}

	// Search for matching transaction using configured deterministic fields or default external_id
	var matchedTx *models.Transaction
	var matchReason string

	if len(policy.DeterministicFields) > 0 && (len(policy.DeterministicFields) != 1 || policy.DeterministicFields[0] != "external_id") {
		// Use configured deterministic fields to search
		for _, field := range policy.DeterministicFields {
			var fieldValue string
			switch field {
			case "external_id":
				fieldValue = tx.ExternalID
			default:
				// Look in metadata
				if tx.Metadata != nil {
					if v, ok := tx.Metadata[field]; ok {
						fieldValue = fmt.Sprintf("%v", v)
					}
				}
			}
			if fieldValue == "" {
				continue
			}

			results, err := s.txStore.SearchByField(ctx, oppositeSide, field, fieldValue)
			if err != nil {
				return nil, fmt.Errorf("failed to search by field %s: %w", field, err)
			}
			if len(results) > 0 {
				matchedTx = results[0]
				matchReason = fmt.Sprintf("deterministic match on field %s=%s", field, fieldValue)
				break
			}
		}
	} else {
		// Default: match by external_id
		var err error
		matchedTx, err = s.txStore.GetByExternalID(ctx, oppositeSide, tx.ExternalID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				matchedTx = nil
			} else {
				return nil, fmt.Errorf("failed to search for match: %w", err)
			}
		}
		matchReason = "deterministic match on external_id"
	}

	if matchedTx == nil {
		return &matching.MatchResult{
			Decision: matching.DecisionUnmatched,
		}, nil
	}

	// Check if either transaction is already part of an existing match for this policy
	existingMatch, err := s.store.FindMatchByTransactionIDs(ctx, policy.ID, []uuid.UUID{tx.ID, matchedTx.ID})
	if err != nil {
		return nil, fmt.Errorf("failed to check existing matches: %w", err)
	}
	if existingMatch != nil {
		return nil, fmt.Errorf("transaction %s or %s is already part of match %s for this policy", tx.ID, matchedTx.ID, existingMatch.ID)
	}

	// Found a match - create the match record
	match := &models.Match{
		ID:       uuid.New(),
		PolicyID: policy.ID,
		Score:    1.0, // Deterministic match has perfect score
		Decision: models.DecisionMatched,
		Explanation: models.Explanation{
			Reason: matchReason,
		},
		CreatedAt: time.Now().UTC(),
	}

	// Set transaction IDs based on sides
	if tx.Side == models.TransactionSideLedger {
		match.LedgerTransactionIDs = []uuid.UUID{tx.ID}
		match.PaymentsTransactionIDs = []uuid.UUID{matchedTx.ID}
	} else {
		match.LedgerTransactionIDs = []uuid.UUID{matchedTx.ID}
		match.PaymentsTransactionIDs = []uuid.UUID{tx.ID}
	}

	// Save the match
	if err := s.store.CreateMatch(ctx, match); err != nil {
		return nil, fmt.Errorf("failed to create match: %w", err)
	}

	return &matching.MatchResult{
		Decision: matching.DecisionMatched,
		Match:    match,
	}, nil
}
