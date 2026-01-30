package service

import (
	"context"
	"testing"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type mockTxStoreCollect struct {
	byProvider map[string][]*models.Transaction
}

func (m *mockTxStoreCollect) Create(ctx context.Context, tx *models.Transaction) error {
	return nil
}

func (m *mockTxStoreCollect) CreateBatch(ctx context.Context, txs []*models.Transaction) ([]bool, error) {
	return nil, nil
}

func (m *mockTxStoreCollect) GetByID(ctx context.Context, id uuid.UUID) (*models.Transaction, error) {
	return nil, nil
}

func (m *mockTxStoreCollect) GetByExternalID(ctx context.Context, side models.TransactionSide, externalID string) (*models.Transaction, error) {
	return nil, nil
}

func (m *mockTxStoreCollect) GetByProvider(ctx context.Context, provider string, side models.TransactionSide) ([]*models.Transaction, error) {
	return m.byProvider[provider], nil
}

func (m *mockTxStoreCollect) SearchByField(ctx context.Context, side models.TransactionSide, field string, value string) ([]*models.Transaction, error) {
	return nil, nil
}

func (m *mockTxStoreCollect) ExistsByExternalIDs(ctx context.Context, side models.TransactionSide, externalIDs []string) (map[string]bool, error) {
	return map[string]bool{}, nil
}

func (m *mockTxStoreCollect) Refresh(ctx context.Context) error {
	return nil
}

func TestCollectPolicyTransactionIDs_IncludesConnectorID(t *testing.T) {
	ctx := context.Background()

	ledgerID := uuid.New()
	legacyID := uuid.New()
	connectorTypeTxID := uuid.New()
	connectorIDTxID := uuid.New()

	ledgerName := "ledger-1"
	connectorType := "stripe"
	connectorIDStr := "conn-123"
	legacyProvider := "legacy-provider"

	txStore := &mockTxStoreCollect{
		byProvider: map[string][]*models.Transaction{
			ledgerName: {
				{ID: ledgerID, Side: models.TransactionSideLedger, Provider: ledgerName},
			},
			legacyProvider: {
				{ID: legacyID, Side: models.TransactionSidePayments, Provider: legacyProvider},
			},
			connectorType: {
				{ID: connectorTypeTxID, Side: models.TransactionSidePayments, Provider: connectorType},
			},
			connectorIDStr: {
				{ID: connectorIDTxID, Side: models.TransactionSidePayments, Provider: connectorIDStr},
			},
		},
	}

	svc := &Service{txStore: txStore}
	policy := &models.Policy{
		LedgerName:       ledgerName,
		PaymentsProvider: legacyProvider,
		ConnectorType:    &connectorType,
		ConnectorID:      &connectorIDStr,
	}

	ids, err := svc.collectPolicyTransactionIDs(ctx, policy)
	require.NoError(t, err)

	expected := []string{
		ledgerID.String(),
		legacyID.String(),
		connectorTypeTxID.String(),
		connectorIDTxID.String(),
	}

	require.Equal(t, expected, ids)
}
