package storage

import (
	"context"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
)

//go:generate mockgen -source transaction_store.go -destination transaction_store_mock.go -package storage . TransactionStore

// TransactionStore defines the interface for transaction storage operations.
// This interface is implemented by OpenSearch as the single source of truth.
type TransactionStore interface {
	// Write operations
	Create(ctx context.Context, tx *models.Transaction) error
	CreateBatch(ctx context.Context, txs []*models.Transaction) error

	// Read operations
	GetByID(ctx context.Context, id uuid.UUID) (*models.Transaction, error)
	GetByExternalID(ctx context.Context, side models.TransactionSide, externalID string) (*models.Transaction, error)
	GetByProvider(ctx context.Context, provider string, side models.TransactionSide) ([]*models.Transaction, error)

	// Search operations
	SearchByField(ctx context.Context, side models.TransactionSide, field string, value string) ([]*models.Transaction, error)

	// Deduplication
	ExistsByExternalIDs(ctx context.Context, side models.TransactionSide, externalIDs []string) (map[string]bool, error)

	// Refresh ensures all pending writes are searchable (useful for tests)
	Refresh(ctx context.Context) error
}
