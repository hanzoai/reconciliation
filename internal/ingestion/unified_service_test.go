package ingestion

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestUnifiedEventHandler_Handle_LedgerEvents(t *testing.T) {
	t.Run("valid ledger event creates transaction", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		// Create a valid ledger event (COMMITTED_TRANSACTIONS)
		ledgerEvent := LedgerEvent{
			Type:   LedgerEventTypeTransactionCreated,
			Ledger: "test-ledger",
			Payload: map[string]interface{}{
				"transactions": []interface{}{
					map[string]interface{}{
						"id":        "ledger-tx-001",
						"timestamp": time.Now().Add(-25 * time.Hour).Format(time.RFC3339),
						"postings": []interface{}{
							map[string]interface{}{
								"amount":      float64(1000),
								"asset":       "USD/2",
								"source":      "world",
								"destination": "users:test",
							},
						},
						"metadata": map[string]interface{}{
							"test_scenario": "unit-test",
						},
					},
				},
			},
		}

		payload, err := json.Marshal(ledgerEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-ledger-001",
			Payload: payload,
		}

		// Expect ExistsByExternalIDs to return not found
		mockStore.EXPECT().
			ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, []string{"ledger-tx-001"}).
			Return(map[string]bool{}, nil)

		// Expect Create to be called
		mockStore.EXPECT().
			Create(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, tx *models.Transaction) error {
				assert.Nil(t, tx.PolicyID)
				assert.Equal(t, "ledger-tx-001", tx.ExternalID)
				assert.Equal(t, "test-ledger", tx.Provider)
				assert.Equal(t, models.TransactionSideLedger, tx.Side)
				assert.Equal(t, int64(1000), tx.Amount)
				assert.Equal(t, "USD/2", tx.Currency)
				return nil
			})

		err = handler.Handle(context.Background(), event)
		assert.NoError(t, err)
	})

	t.Run("duplicate ledger event skips without error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		ledgerEvent := LedgerEvent{
			Type:   LedgerEventTypeTransactionCreated,
			Ledger: "test-ledger",
			Payload: map[string]interface{}{
				"transactions": []interface{}{
					map[string]interface{}{
						"id":        "ledger-tx-duplicate",
						"timestamp": time.Now().Format(time.RFC3339),
						"postings": []interface{}{
							map[string]interface{}{
								"amount":      float64(500),
								"asset":       "EUR/2",
								"source":      "world",
								"destination": "users:test",
							},
						},
					},
				},
			},
		}

		payload, err := json.Marshal(ledgerEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-ledger-002",
			Payload: payload,
		}

		// Return that ID already exists
		mockStore.EXPECT().
			ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, []string{"ledger-tx-duplicate"}).
			Return(map[string]bool{"ledger-tx-duplicate": true}, nil)

		err = handler.Handle(context.Background(), event)
		assert.NoError(t, err)
	})
}

func TestUnifiedEventHandler_Handle_PaymentEvents(t *testing.T) {
	t.Run("valid payment event creates transaction", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		// Create a valid payments event (SAVED_PAYMENT)
		paymentsEvent := PaymentsEvent{
			Type: PaymentsEventTypeSavedPayment,
			Payload: map[string]interface{}{
				"id":        "payment-001",
				"connector": "stripe",
				"amount":    float64(2000),
				"asset":     "USD/2",
				"createdAt": time.Now().Format(time.RFC3339),
				"status":    "succeeded",
				"metadata": map[string]interface{}{
					"order_id": "order-789",
				},
			},
		}

		payload, err := json.Marshal(paymentsEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-payment-001",
			Payload: payload,
		}

		// Expect ExistsByExternalIDs to return not found
		mockStore.EXPECT().
			ExistsByExternalIDs(gomock.Any(), models.TransactionSidePayments, []string{"payment-001"}).
			Return(map[string]bool{}, nil)

		// Expect Create to be called
		mockStore.EXPECT().
			Create(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, tx *models.Transaction) error {
				assert.Nil(t, tx.PolicyID)
				assert.Equal(t, "payment-001", tx.ExternalID)
				assert.Equal(t, "stripe", tx.Provider)
				assert.Equal(t, models.TransactionSidePayments, tx.Side)
				assert.Equal(t, int64(2000), tx.Amount)
				assert.Equal(t, "USD/2", tx.Currency)
				return nil
			})

		err = handler.Handle(context.Background(), event)
		assert.NoError(t, err)
	})

	t.Run("duplicate payment event skips without error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		paymentsEvent := PaymentsEvent{
			Type: PaymentsEventTypeSavedPayment,
			Payload: map[string]interface{}{
				"id":        "payment-duplicate",
				"connector": "stripe",
				"amount":    float64(500),
				"asset":     "EUR/2",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		payload, err := json.Marshal(paymentsEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-payment-002",
			Payload: payload,
		}

		mockStore.EXPECT().
			ExistsByExternalIDs(gomock.Any(), models.TransactionSidePayments, []string{"payment-duplicate"}).
			Return(map[string]bool{"payment-duplicate": true}, nil)

		err = handler.Handle(context.Background(), event)
		assert.NoError(t, err)
	})
}

func TestUnifiedEventHandler_Handle_Routing(t *testing.T) {
	t.Run("routes COMMITTED_TRANSACTIONS to ledger handler", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		ledgerEvent := LedgerEvent{
			Type:   LedgerEventTypeTransactionCreated,
			Ledger: "routing-test-ledger",
			Payload: map[string]interface{}{
				"transactions": []interface{}{
					map[string]interface{}{
						"id":        "routing-ledger-tx",
						"timestamp": time.Now().Format(time.RFC3339),
						"postings": []interface{}{
							map[string]interface{}{
								"amount":      float64(100),
								"asset":       "USD/2",
								"source":      "world",
								"destination": "users:test",
							},
						},
					},
				},
			},
		}

		payload, err := json.Marshal(ledgerEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-routing-ledger",
			Payload: payload,
		}

		// Verify it goes to ledger side
		mockStore.EXPECT().
			ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, []string{"routing-ledger-tx"}).
			Return(map[string]bool{}, nil)

		mockStore.EXPECT().
			Create(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, tx *models.Transaction) error {
				assert.Equal(t, models.TransactionSideLedger, tx.Side)
				return nil
			})

		err = handler.Handle(context.Background(), event)
		assert.NoError(t, err)
	})

	t.Run("routes SAVED_PAYMENT to payments handler", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		paymentsEvent := PaymentsEvent{
			Type: PaymentsEventTypeSavedPayment,
			Payload: map[string]interface{}{
				"id":        "routing-payment-tx",
				"connector": "stripe",
				"amount":    float64(200),
				"asset":     "USD/2",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		payload, err := json.Marshal(paymentsEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-routing-payment",
			Payload: payload,
		}

		// Verify it goes to payments side
		mockStore.EXPECT().
			ExistsByExternalIDs(gomock.Any(), models.TransactionSidePayments, []string{"routing-payment-tx"}).
			Return(map[string]bool{}, nil)

		mockStore.EXPECT().
			Create(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, tx *models.Transaction) error {
				assert.Equal(t, models.TransactionSidePayments, tx.Side)
				return nil
			})

		err = handler.Handle(context.Background(), event)
		assert.NoError(t, err)
	})

	t.Run("ignores non-relevant event types", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		// AUDIT event should be ignored
		auditEvent := map[string]interface{}{
			"type": "AUDIT",
			"payload": map[string]interface{}{
				"some_data": "value",
			},
		}

		payload, err := json.Marshal(auditEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-audit",
			Payload: payload,
		}

		// No store calls should be made
		err = handler.Handle(context.Background(), event)
		assert.NoError(t, err)
	})

	t.Run("ignores unknown event types", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		unknownEvent := map[string]interface{}{
			"type": "SOME_UNKNOWN_TYPE",
			"payload": map[string]interface{}{
				"data": "value",
			},
		}

		payload, err := json.Marshal(unknownEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-unknown",
			Payload: payload,
		}

		// No store calls should be made
		err = handler.Handle(context.Background(), event)
		assert.NoError(t, err)
	})
}

func TestUnifiedEventHandler_Handle_ErrorCases(t *testing.T) {
	t.Run("invalid JSON payload skips without error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		event := &GenericEvent{
			ID:      "event-invalid-json",
			Payload: []byte("this is not valid json"),
		}

		err := handler.Handle(context.Background(), event)
		assert.NoError(t, err)
	})

	t.Run("non-GenericEvent type skips without error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		err := handler.Handle(context.Background(), "not a generic event")
		assert.NoError(t, err)
	})

	t.Run("store error on check returns retryable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		paymentsEvent := PaymentsEvent{
			Type: PaymentsEventTypeSavedPayment,
			Payload: map[string]interface{}{
				"id":        "payment-store-error",
				"connector": "stripe",
				"amount":    float64(100),
				"asset":     "USD/2",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		payload, err := json.Marshal(paymentsEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-store-error",
			Payload: payload,
		}

		storeError := errors.New("opensearch connection lost")
		mockStore.EXPECT().
			ExistsByExternalIDs(gomock.Any(), models.TransactionSidePayments, []string{"payment-store-error"}).
			Return(nil, storeError)

		err = handler.Handle(context.Background(), event)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrRetryable))
	})

	t.Run("store error on create returns retryable error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)
		handler := NewUnifiedEventHandler(mockStore, nil)

		paymentsEvent := PaymentsEvent{
			Type: PaymentsEventTypeSavedPayment,
			Payload: map[string]interface{}{
				"id":        "payment-create-error",
				"connector": "stripe",
				"amount":    float64(200),
				"asset":     "USD/2",
				"createdAt": time.Now().Format(time.RFC3339),
			},
		}

		payload, err := json.Marshal(paymentsEvent)
		require.NoError(t, err)

		event := &GenericEvent{
			ID:      "event-create-error",
			Payload: payload,
		}

		mockStore.EXPECT().
			ExistsByExternalIDs(gomock.Any(), models.TransactionSidePayments, []string{"payment-create-error"}).
			Return(map[string]bool{}, nil)

		storeError := errors.New("opensearch write failed")
		mockStore.EXPECT().
			Create(gomock.Any(), gomock.Any()).
			Return(storeError)

		err = handler.Handle(context.Background(), event)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrRetryable))
	})
}

func TestUnifiedIngestionService(t *testing.T) {
	t.Run("service delegates to consumer", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)

		consumer := &GenericConsumer{}
		consumer.running.Store(true)
		consumer.healthy.Store(true)

		config := UnifiedIngestionServiceConfig{}

		service := NewUnifiedIngestionService(consumer, mockStore, config)

		// Test Health delegation
		assert.NoError(t, service.Health())

		// Make consumer unhealthy
		consumer.healthy.Store(false)
		assert.Error(t, service.Health())
	})

	t.Run("getConsumer returns underlying consumer", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStore := storage.NewMockTransactionStore(ctrl)

		consumer := &GenericConsumer{}
		config := UnifiedIngestionServiceConfig{}

		service := NewUnifiedIngestionService(consumer, mockStore, config)

		// getConsumer should return the consumer
		assert.Equal(t, consumer, service.getConsumer())
	})
}

// mockTransactionStore for testing purposes.
type mockTransactionStore struct{}

func (m *mockTransactionStore) Create(ctx context.Context, tx *models.Transaction) error {
	return nil
}

func (m *mockTransactionStore) CreateBatch(ctx context.Context, txs []*models.Transaction) error {
	return nil
}

func (m *mockTransactionStore) GetByID(ctx context.Context, id uuid.UUID) (*models.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionStore) GetByExternalID(ctx context.Context, side models.TransactionSide, externalID string) (*models.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionStore) GetByProvider(ctx context.Context, provider string, side models.TransactionSide) ([]*models.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionStore) SearchByField(ctx context.Context, side models.TransactionSide, field string, value string) ([]*models.Transaction, error) {
	return nil, nil
}

func (m *mockTransactionStore) ExistsByExternalIDs(ctx context.Context, side models.TransactionSide, externalIDs []string) (map[string]bool, error) {
	return nil, nil
}

func (m *mockTransactionStore) Refresh(ctx context.Context) error {
	return nil
}

// Ensure mockTransactionStore implements storage.TransactionStore.
var _ storage.TransactionStore = (*mockTransactionStore)(nil)
