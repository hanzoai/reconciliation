package matching

import (
	"context"
	"errors"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
)

// StoragePolicyResolver resolves policies using the Storage layer (PostgreSQL).
// For LEDGER transactions, it looks up by ledger name (provider).
// For PAYMENTS transactions, it tries payments_provider first, then connector_type.
type StoragePolicyResolver struct {
	store *storage.Storage
}

// NewStoragePolicyResolver creates a new StoragePolicyResolver.
func NewStoragePolicyResolver(store *storage.Storage) *StoragePolicyResolver {
	return &StoragePolicyResolver{store: store}
}

// ResolvePolicy finds the matching transactional policy for a provider and side.
// Returns nil, nil if no policy is found (not an error, just no matching policy).
func (r *StoragePolicyResolver) ResolvePolicy(ctx context.Context, provider string, side models.TransactionSide) (*models.Policy, error) {
	switch side {
	case models.TransactionSideLedger:
		return r.resolveLedgerPolicy(ctx, provider)
	case models.TransactionSidePayments:
		return r.resolvePaymentsPolicy(ctx, provider)
	default:
		return nil, nil
	}
}

func (r *StoragePolicyResolver) resolveLedgerPolicy(ctx context.Context, ledgerName string) (*models.Policy, error) {
	policy, err := r.store.GetPolicyByLedgerName(ctx, ledgerName)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return policy, nil
}

func (r *StoragePolicyResolver) resolvePaymentsPolicy(ctx context.Context, provider string) (*models.Policy, error) {
	// Try payments_provider first
	policy, err := r.store.GetPolicyByPaymentsProvider(ctx, provider)
	if err == nil {
		return policy, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	// Fallback to connector_type
	policy, err = r.store.GetPolicyByConnectorType(ctx, provider)
	if err == nil {
		return policy, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}

	return nil, nil
}
