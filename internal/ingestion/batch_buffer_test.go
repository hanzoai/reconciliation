package ingestion

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestBatchBufferConfigFromEnv(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		config := BatchBufferConfigFromEnv()
		assert.Equal(t, DefaultBatchBufferSize, config.BufferSize)
		assert.Equal(t, time.Duration(DefaultBatchFlushIntervalMS)*time.Millisecond, config.FlushInterval)
		assert.Equal(t, DefaultBatchEnabled, config.Enabled)
	})

	t.Run("custom values from env", func(t *testing.T) {
		t.Setenv(BatchBufferSizeEnvVar, "50")
		t.Setenv(BatchFlushIntervalMSEnvVar, "500")
		t.Setenv(BatchEnabledEnvVar, "false")

		config := BatchBufferConfigFromEnv()
		assert.Equal(t, 50, config.BufferSize)
		assert.Equal(t, 500*time.Millisecond, config.FlushInterval)
		assert.False(t, config.Enabled)
	})

	t.Run("invalid values fallback to defaults", func(t *testing.T) {
		t.Setenv(BatchBufferSizeEnvVar, "invalid")
		t.Setenv(BatchFlushIntervalMSEnvVar, "-1")

		config := BatchBufferConfigFromEnv()
		assert.Equal(t, DefaultBatchBufferSize, config.BufferSize)
		assert.Equal(t, time.Duration(DefaultBatchFlushIntervalMS)*time.Millisecond, config.FlushInterval)
	})
}

func TestBatchBufferMetrics(t *testing.T) {
	metrics, err := NewBatchBufferMetrics()
	require.NoError(t, err)
	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.flushTotal)
	assert.NotNil(t, metrics.flushSize)
	assert.NotNil(t, metrics.flushDuration)
	assert.NotNil(t, metrics.ackTotal)
	assert.NotNil(t, metrics.nackTotal)
	assert.NotNil(t, metrics.bufferSize)
}

func TestNewBatchBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    10,
		FlushInterval: 100 * time.Millisecond,
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	assert.NotNil(t, buffer)
	assert.Equal(t, config.BufferSize, buffer.config.BufferSize)
	assert.Equal(t, config.FlushInterval, buffer.config.FlushInterval)
	assert.True(t, buffer.IsEnabled())
}

func TestBatchBuffer_Submit_Disabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    10,
		FlushInterval: 100 * time.Millisecond,
		Enabled:       false, // Disabled
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	tx := createTestTransaction(models.TransactionSideLedger)
	msg := createTestMessage()

	// When batch mode is disabled, it should process synchronously
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), tx.Side, []string{tx.ExternalID}).
		Return(map[string]bool{}, nil)

	mockStore.EXPECT().
		Create(gomock.Any(), tx).
		Return(nil)

	buffer.Submit(context.Background(), msg, tx)

	// Message should be Acked immediately
	assert.True(t, isAcked(msg))
}

func TestBatchBuffer_FlushBySize(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    3,             // Small buffer for testing
		FlushInterval: 1 * time.Hour, // Long interval so only size triggers flush
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, buffer.Start(ctx))

	// Create 3 transactions (equal to buffer size)
	txs := make([]*models.Transaction, 3)
	msgs := make([]*message.Message, 3)

	for i := 0; i < 3; i++ {
		txs[i] = createTestTransaction(models.TransactionSideLedger)
		msgs[i] = createTestMessage()
	}

	// Expect duplicate check for all external IDs
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, gomock.Any()).
		Return(map[string]bool{}, nil)

	// Expect batch insert
	mockStore.EXPECT().
		CreateBatch(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, txs []*models.Transaction) ([]bool, error) {
			results := make([]bool, len(txs))
			for i := range results {
				results[i] = true
			}
			return results, nil
		})

	// Submit all items
	for i := 0; i < 3; i++ {
		buffer.Submit(ctx, msgs[i], txs[i])
	}

	// Wait for flush to complete
	require.Eventually(t, func() bool {
		for _, msg := range msgs {
			if !isAcked(msg) {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, buffer.Stop())
}

func TestBatchBuffer_FlushByTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    100,                   // Large buffer
		FlushInterval: 50 * time.Millisecond, // Short interval
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, buffer.Start(ctx))

	// Create 1 transaction (less than buffer size)
	tx := createTestTransaction(models.TransactionSideLedger)
	msg := createTestMessage()

	// Expect duplicate check
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, gomock.Any()).
		Return(map[string]bool{}, nil)

	// Expect batch insert
	mockStore.EXPECT().
		CreateBatch(gomock.Any(), gomock.Any()).
		DoAndReturn(allSuccessCreateBatch)

	buffer.Submit(ctx, msg, tx)

	// Wait for timeout flush
	require.Eventually(t, func() bool {
		return isAcked(msg)
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, buffer.Stop())
}

func TestBatchBuffer_DuplicateHandling(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    10,
		FlushInterval: 50 * time.Millisecond,
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, buffer.Start(ctx))

	tx := createTestTransaction(models.TransactionSideLedger)
	msg := createTestMessage()

	// Expect duplicate check - return that ID already exists
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, gomock.Any()).
		Return(map[string]bool{tx.ExternalID: true}, nil)

	// Should NOT call CreateBatch since it's a duplicate

	buffer.Submit(ctx, msg, tx)

	// Wait for flush
	require.Eventually(t, func() bool {
		return isAcked(msg)
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, buffer.Stop())
}

func TestBatchBuffer_DBError_NackAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    10,
		FlushInterval: 50 * time.Millisecond,
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, buffer.Start(ctx))

	tx := createTestTransaction(models.TransactionSideLedger)
	msg := createTestMessage()

	// Expect duplicate check - return error
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, gomock.Any()).
		Return(nil, assert.AnError)

	buffer.Submit(ctx, msg, tx)

	// Wait for flush and stop buffer to ensure all processing is complete
	require.NoError(t, buffer.Stop())

	// Message should be Nacked due to DB error
	assert.True(t, isNacked(msg))
}

func TestBatchBuffer_InsertError_NackAll(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    10,
		FlushInterval: 50 * time.Millisecond,
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, buffer.Start(ctx))

	tx := createTestTransaction(models.TransactionSideLedger)
	msg := createTestMessage()

	// Expect duplicate check - no duplicates
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, gomock.Any()).
		Return(map[string]bool{}, nil)

	// Expect batch insert - return error
	mockStore.EXPECT().
		CreateBatch(gomock.Any(), gomock.Any()).
		Return(nil, assert.AnError)

	buffer.Submit(ctx, msg, tx)

	// Wait for flush
	require.Eventually(t, func() bool {
		return isNacked(msg)
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, buffer.Stop())
}

func TestBatchBuffer_SideSeparation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    10,
		FlushInterval: 50 * time.Millisecond,
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, buffer.Start(ctx))

	// Create one ledger and one payment transaction
	ledgerTx := createTestTransaction(models.TransactionSideLedger)
	ledgerMsg := createTestMessage()

	paymentTx := createTestTransaction(models.TransactionSidePayments)
	paymentMsg := createTestMessage()

	// Expect separate duplicate checks for each side
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, gomock.Any()).
		Return(map[string]bool{}, nil)

	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), models.TransactionSidePayments, gomock.Any()).
		Return(map[string]bool{}, nil)

	// Expect separate batch inserts for each side
	mockStore.EXPECT().
		CreateBatch(gomock.Any(), gomock.Any()).
		DoAndReturn(allSuccessCreateBatch).
		Times(2)

	buffer.Submit(ctx, ledgerMsg, ledgerTx)
	buffer.Submit(ctx, paymentMsg, paymentTx)

	// Wait for flush
	require.Eventually(t, func() bool {
		return isAcked(ledgerMsg) && isAcked(paymentMsg)
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, buffer.Stop())
}

func TestBatchBuffer_InBatchDeduplication(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    10,
		FlushInterval: 50 * time.Millisecond,
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, buffer.Start(ctx))

	// Create two transactions with the same external ID
	tx1 := createTestTransaction(models.TransactionSideLedger)
	msg1 := createTestMessage()

	tx2 := createTestTransaction(models.TransactionSideLedger)
	tx2.ExternalID = tx1.ExternalID // Same external ID
	msg2 := createTestMessage()

	// Expect duplicate check - return no existing
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, gomock.Any()).
		Return(map[string]bool{}, nil)

	// Expect only ONE transaction to be inserted (second is deduplicated in-batch)
	mockStore.EXPECT().
		CreateBatch(gomock.Any(), gomock.Len(1)).
		DoAndReturn(allSuccessCreateBatch)

	buffer.Submit(ctx, msg1, tx1)
	buffer.Submit(ctx, msg2, tx2)

	// Wait for flush
	require.Eventually(t, func() bool {
		return isAcked(msg1) && isAcked(msg2)
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, buffer.Stop())
}

func TestBatchBuffer_ConcurrentSubmit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    100,
		FlushInterval: 100 * time.Millisecond,
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, buffer.Start(ctx))

	// Allow multiple calls due to concurrency
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(map[string]bool{}, nil).
		AnyTimes()

	mockStore.EXPECT().
		CreateBatch(gomock.Any(), gomock.Any()).
		DoAndReturn(allSuccessCreateBatch).
		AnyTimes()

	// Submit from multiple goroutines
	var wg sync.WaitGroup
	numGoroutines := 10
	numMessagesPerGoroutine := 5

	msgs := make([]*message.Message, numGoroutines*numMessagesPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < numMessagesPerGoroutine; i++ {
				idx := goroutineID*numMessagesPerGoroutine + i
				tx := createTestTransaction(models.TransactionSideLedger)
				msgs[idx] = createTestMessage()
				buffer.Submit(ctx, msgs[idx], tx)
			}
		}(g)
	}

	wg.Wait()

	// Wait for flush
	require.Eventually(t, func() bool {
		for _, msg := range msgs {
			if msg == nil || !isAcked(msg) {
				return false
			}
		}
		return true
	}, 2*time.Second, 10*time.Millisecond)

	// No panic or race condition should occur
	require.NoError(t, buffer.Stop())
}

func TestBatchBuffer_ShutdownFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := storage.NewMockTransactionStore(ctrl)

	config := BatchBufferConfig{
		BufferSize:    100,           // Large buffer
		FlushInterval: 1 * time.Hour, // Long interval
		Enabled:       true,
	}

	buffer := NewBatchBuffer(config, mockStore, nil, nil, nil, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, buffer.Start(ctx))

	tx := createTestTransaction(models.TransactionSideLedger)
	msg := createTestMessage()

	// Submit without triggering normal flush
	buffer.Submit(ctx, msg, tx)

	// Expect flush on shutdown
	mockStore.EXPECT().
		ExistsByExternalIDs(gomock.Any(), models.TransactionSideLedger, gomock.Any()).
		Return(map[string]bool{}, nil)

	mockStore.EXPECT().
		CreateBatch(gomock.Any(), gomock.Any()).
		DoAndReturn(allSuccessCreateBatch)

	// Stop should trigger final flush
	require.NoError(t, buffer.Stop())

	assert.True(t, isAcked(msg))
}

// Helper functions

func createTestTransaction(side models.TransactionSide) *models.Transaction {
	return &models.Transaction{
		ID:         uuid.New(),
		Side:       side,
		Provider:   "test-provider",
		ExternalID: uuid.New().String(),
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now(),
		IngestedAt: time.Now(),
		Metadata:   map[string]interface{}{"test": true},
	}
}

func createTestMessage() *message.Message {
	return message.NewMessage(uuid.NewString(), []byte("test"))
}

// isAcked checks if a message has been acknowledged.
func isAcked(m *message.Message) bool {
	select {
	case <-m.Acked():
		return true
	default:
		return false
	}
}

// isNacked checks if a message has been nacked.
func isNacked(m *message.Message) bool {
	select {
	case <-m.Nacked():
		return true
	default:
		return false
	}
}

// allSuccessCreateBatch is a DoAndReturn helper that returns all-true results for CreateBatch.
func allSuccessCreateBatch(_ context.Context, txs []*models.Transaction) ([]bool, error) {
	results := make([]bool, len(txs))
	for i := range results {
		results[i] = true
	}
	return results, nil
}
