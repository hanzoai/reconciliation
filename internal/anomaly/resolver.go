package anomaly

import (
	"context"
	"fmt"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

// DefaultResolver is the default implementation of the matching.AnomalyResolver interface.
// It resolves open anomalies when a late match is found.
type DefaultResolver struct {
	anomalyRepo storage.AnomalyRepository
}

// NewDefaultResolver creates a new DefaultResolver instance.
func NewDefaultResolver(anomalyRepo storage.AnomalyRepository) *DefaultResolver {
	return &DefaultResolver{
		anomalyRepo: anomalyRepo,
	}
}

// ResolveForMatch checks if any transactions in the match have open anomalies
// and resolves them automatically with the reason "Resolved by match {match_id}".
func (r *DefaultResolver) ResolveForMatch(ctx context.Context, match *models.Match) error {
	if match == nil {
		return nil
	}

	// Collect all transaction IDs from the match
	transactionIDs := make([]uuid.UUID, 0, len(match.LedgerTransactionIDs)+len(match.PaymentsTransactionIDs))
	transactionIDs = append(transactionIDs, match.LedgerTransactionIDs...)
	transactionIDs = append(transactionIDs, match.PaymentsTransactionIDs...)

	if len(transactionIDs) == 0 {
		return nil
	}

	// Find open anomalies for these transactions
	anomalies, err := r.anomalyRepo.FindOpenByTransactionIDs(ctx, transactionIDs)
	if err != nil {
		return fmt.Errorf("failed to find open anomalies: %w", err)
	}

	// No anomalies to resolve
	if len(anomalies) == 0 {
		return nil
	}

	// Resolve each anomaly with the match ID as the reason
	resolvedBy := fmt.Sprintf("Resolved by match %s", match.ID.String())
	for _, anomaly := range anomalies {
		if err := r.anomalyRepo.Resolve(ctx, anomaly.ID, resolvedBy); err != nil {
			return fmt.Errorf("failed to resolve anomaly %s: %w", anomaly.ID.String(), err)
		}
	}

	return nil
}
