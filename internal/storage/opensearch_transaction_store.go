package storage

import (
	"context"

	"github.com/formancehq/reconciliation/internal/elasticsearch"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
)

// OpenSearchTransactionStore implements TransactionStore using OpenSearch.
type OpenSearchTransactionStore struct {
	client *elasticsearch.Client
	stack  string
}

// NewOpenSearchTransactionStore creates a new OpenSearchTransactionStore.
func NewOpenSearchTransactionStore(client *elasticsearch.Client, stack string) *OpenSearchTransactionStore {
	return &OpenSearchTransactionStore{
		client: client,
		stack:  stack,
	}
}

// Create indexes a single transaction in OpenSearch.
func (s *OpenSearchTransactionStore) Create(ctx context.Context, tx *models.Transaction) error {
	return s.client.CreateTransaction(ctx, s.stack, tx)
}

// CreateBatch indexes multiple transactions in OpenSearch using bulk API.
// Uses idempotent document IDs ({side}_{external_id}) to prevent duplicates.
func (s *OpenSearchTransactionStore) CreateBatch(ctx context.Context, txs []*models.Transaction) error {
	return s.client.BulkIndexIdempotent(ctx, s.stack, txs)
}

// GetByID retrieves a transaction by its UUID.
func (s *OpenSearchTransactionStore) GetByID(ctx context.Context, id uuid.UUID) (*models.Transaction, error) {
	tx, err := s.client.GetTransactionByID(ctx, s.stack, id)
	if err != nil {
		if err == elasticsearch.ErrTransactionNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return tx, nil
}

// GetByExternalID retrieves a transaction by side and external_id.
func (s *OpenSearchTransactionStore) GetByExternalID(ctx context.Context, side models.TransactionSide, externalID string) (*models.Transaction, error) {
	tx, err := s.client.GetTransactionByExternalID(ctx, s.stack, side, externalID)
	if err != nil {
		if err == elasticsearch.ErrTransactionNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return tx, nil
}

// GetByProvider returns all transactions for a given provider and side.
func (s *OpenSearchTransactionStore) GetByProvider(ctx context.Context, provider string, side models.TransactionSide) ([]*models.Transaction, error) {
	return s.client.GetTransactionsByProvider(ctx, s.stack, provider, side)
}

// SearchByField searches transactions by any field (direct or metadata).
func (s *OpenSearchTransactionStore) SearchByField(ctx context.Context, side models.TransactionSide, field string, value string) ([]*models.Transaction, error) {
	return s.client.SearchByField(ctx, s.stack, side, field, value)
}

// ExistsByExternalIDs checks which external_ids already exist in OpenSearch.
func (s *OpenSearchTransactionStore) ExistsByExternalIDs(ctx context.Context, side models.TransactionSide, externalIDs []string) (map[string]bool, error) {
	return s.client.ExistsByExternalIDs(ctx, s.stack, side, externalIDs)
}

// Refresh ensures all pending writes are searchable.
func (s *OpenSearchTransactionStore) Refresh(ctx context.Context) error {
	indexPattern := elasticsearch.TransactionIndexPattern(s.stack)
	return s.client.RefreshIndex(ctx, indexPattern)
}

// Ensure OpenSearchTransactionStore implements TransactionStore.
var _ TransactionStore = (*OpenSearchTransactionStore)(nil)
