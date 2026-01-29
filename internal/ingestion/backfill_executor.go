package ingestion

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/operations"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/shared"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/events"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

// BackfillSDK defines the SDK methods needed by the backfill executor.
type BackfillSDK interface {
	V2ListLedgers(ctx context.Context, req operations.V2ListLedgersRequest) (*operations.V2ListLedgersResponse, error)
	V2ListTransactions(ctx context.Context, req operations.V2ListTransactionsRequest) (*operations.V2ListTransactionsResponse, error)
	ListPayments(ctx context.Context, req operations.ListPaymentsRequest) (*operations.ListPaymentsResponse, error)
}

// BackfillStore defines the storage methods needed by the backfill executor.
type BackfillStore interface {
	GetBackfill(ctx context.Context, id uuid.UUID) (*models.Backfill, error)
	UpdateBackfillStatus(ctx context.Context, id uuid.UUID, status models.BackfillStatus, errorMessage *string) error
	UpdateBackfillProgress(ctx context.Context, id uuid.UUID, ingested int64, lastCursor *string) error
}

// BackfillExecutor handles the execution of backfill operations.
type BackfillExecutor struct {
	sdk       BackfillSDK
	store     BackfillStore
	txStore   storage.TransactionStore
	publisher message.Publisher
	topic     string
}

// NewBackfillExecutor creates a new BackfillExecutor.
func NewBackfillExecutor(
	sdk BackfillSDK,
	store BackfillStore,
	txStore storage.TransactionStore,
	publisher message.Publisher,
	topic string,
) *BackfillExecutor {
	return &BackfillExecutor{
		sdk:       sdk,
		store:     store,
		txStore:   txStore,
		publisher: publisher,
		topic:     topic,
	}
}

// Execute runs a backfill by ID.
func (e *BackfillExecutor) Execute(ctx context.Context, backfillID uuid.UUID) error {
	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"backfill_id": backfillID.String(),
	})

	backfill, err := e.store.GetBackfill(ctx, backfillID)
	if err != nil {
		logger.Errorf("failed to get backfill: %v", err)
		return err
	}

	// Mark as running
	if err := e.store.UpdateBackfillStatus(ctx, backfillID, models.BackfillStatusRunning, nil); err != nil {
		logger.Errorf("failed to update backfill status to running: %v", err)
		return err
	}

	logger.WithFields(map[string]interface{}{
		"source": backfill.Source,
		"since":  backfill.Since,
	}).Info("Starting backfill execution")

	var execErr error
	switch backfill.Source {
	case models.BackfillSourceLedger:
		execErr = e.executeLedgerBackfill(ctx, backfill)
	case models.BackfillSourcePayments:
		execErr = e.executePaymentsBackfill(ctx, backfill)
	default:
		execErr = fmt.Errorf("unsupported source: %s", backfill.Source)
	}

	if execErr != nil {
		errMsg := execErr.Error()
		_ = e.store.UpdateBackfillStatus(ctx, backfillID, models.BackfillStatusFailed, &errMsg)
		logger.Errorf("backfill failed: %v", execErr)
		return execErr
	}

	if err := e.store.UpdateBackfillStatus(ctx, backfillID, models.BackfillStatusCompleted, nil); err != nil {
		logger.Errorf("failed to mark backfill as completed: %v", err)
		return err
	}

	logger.Info("Backfill completed successfully")
	return nil
}

func (e *BackfillExecutor) executeLedgerBackfill(ctx context.Context, backfill *models.Backfill) error {
	logger := logging.FromContext(ctx)

	// If a specific ledger is set, only backfill that one
	if backfill.Ledger != nil && *backfill.Ledger != "" {
		return e.backfillLedger(ctx, backfill, *backfill.Ledger)
	}

	// Otherwise, list all ledgers and backfill each
	var cursor *string
	for {
		resp, err := e.sdk.V2ListLedgers(ctx, operations.V2ListLedgersRequest{
			Cursor:   cursor,
			PageSize: int64Ptr(100),
		})
		if err != nil {
			return fmt.Errorf("failed to list ledgers: %w", err)
		}
		if resp.V2LedgerListResponse == nil {
			break
		}

		for _, ledger := range resp.V2LedgerListResponse.Cursor.Data {
			logger.WithFields(map[string]interface{}{
				"ledger": ledger.Name,
			}).Info("Backfilling ledger")

			if err := e.backfillLedger(ctx, backfill, ledger.Name); err != nil {
				return fmt.Errorf("failed to backfill ledger %s: %w", ledger.Name, err)
			}
		}

		if !resp.V2LedgerListResponse.Cursor.HasMore {
			break
		}
		cursor = resp.V2LedgerListResponse.Cursor.Next
	}

	return nil
}

func (e *BackfillExecutor) backfillLedger(ctx context.Context, backfill *models.Backfill, ledgerName string) error {
	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"ledger": ledgerName,
	})

	var cursor *string
	var ingested int64

	// Restore progress if available
	if backfill.LastCursor != nil {
		cursor = backfill.LastCursor
	}
	ingested = backfill.Ingested

	pageSize := int64(100)

	for {
		resp, err := e.sdk.V2ListTransactions(ctx, operations.V2ListTransactionsRequest{
			Ledger:   ledgerName,
			Cursor:   cursor,
			PageSize: &pageSize,
			Pit:      &backfill.Since,
		})
		if err != nil {
			return fmt.Errorf("failed to list transactions: %w", err)
		}
		if resp.V2TransactionsCursorResponse == nil {
			break
		}

		txCursor := resp.V2TransactionsCursorResponse.Cursor

		for _, sdkTx := range txCursor.Data {
			tx := e.convertLedgerTransaction(sdkTx, ledgerName)
			if tx == nil {
				continue
			}

			if err := e.ingestTransaction(ctx, tx); err != nil {
				logger.WithFields(map[string]interface{}{
					"external_id": tx.ExternalID,
					"error":       err.Error(),
				}).Error("failed to ingest transaction, continuing")
				continue
			}
			ingested++
		}

		// Update progress
		if err := e.store.UpdateBackfillProgress(ctx, backfill.ID, ingested, txCursor.Next); err != nil {
			logger.Errorf("failed to update progress: %v", err)
		}

		if !txCursor.HasMore {
			break
		}
		cursor = txCursor.Next
	}

	return nil
}

func (e *BackfillExecutor) executePaymentsBackfill(ctx context.Context, backfill *models.Backfill) error {
	logger := logging.FromContext(ctx)

	var cursor *string
	var ingested int64

	// Restore progress if available
	if backfill.LastCursor != nil {
		cursor = backfill.LastCursor
	}
	ingested = backfill.Ingested

	pageSize := int64(100)

	for {
		resp, err := e.sdk.ListPayments(ctx, operations.ListPaymentsRequest{
			Cursor:   cursor,
			PageSize: &pageSize,
		})
		if err != nil {
			return fmt.Errorf("failed to list payments: %w", err)
		}
		if resp.PaymentsCursor == nil {
			break
		}

		pmtCursor := resp.PaymentsCursor.Cursor

		for _, payment := range pmtCursor.Data {
			// Filter by since date
			if payment.CreatedAt.Before(backfill.Since) {
				continue
			}

			tx := e.convertPayment(payment)
			if tx == nil {
				continue
			}

			if err := e.ingestTransaction(ctx, tx); err != nil {
				logger.WithFields(map[string]interface{}{
					"external_id": tx.ExternalID,
					"error":       err.Error(),
				}).Error("failed to ingest payment, continuing")
				continue
			}
			ingested++
		}

		// Update progress
		if err := e.store.UpdateBackfillProgress(ctx, backfill.ID, ingested, pmtCursor.Next); err != nil {
			logger.Errorf("failed to update progress: %v", err)
		}

		if !pmtCursor.HasMore {
			break
		}
		cursor = pmtCursor.Next
	}

	return nil
}

func (e *BackfillExecutor) convertLedgerTransaction(sdkTx shared.V2ExpandedTransaction, ledgerName string) *models.Transaction {
	if sdkTx.ID == nil {
		return nil
	}

	// Aggregate amount from postings
	var totalAmount int64
	var currency string
	for _, posting := range sdkTx.Postings {
		if posting.Amount != nil {
			totalAmount += posting.Amount.Int64()
		}
		if currency == "" {
			currency = posting.Asset
		}
	}

	// Convert metadata from map[string]string to map[string]interface{}
	metadata := make(map[string]interface{})
	for k, v := range sdkTx.Metadata {
		metadata[k] = v
	}

	return &models.Transaction{
		ID:         uuid.New(),
		Side:       models.TransactionSideLedger,
		Provider:   ledgerName,
		ExternalID: sdkTx.ID.String(),
		Amount:     totalAmount,
		Currency:   currency,
		OccurredAt: sdkTx.Timestamp,
		IngestedAt: time.Now(),
		Metadata:   metadata,
	}
}

func (e *BackfillExecutor) convertPayment(payment shared.Payment) *models.Transaction {
	if payment.ID == "" {
		return nil
	}

	var amount int64
	if payment.Amount != nil {
		amount = payment.Amount.Int64()
	}

	// Convert metadata
	metadata := make(map[string]interface{})
	for k, v := range payment.Metadata {
		metadata[k] = v
	}
	if payment.Reference != "" {
		metadata["reference"] = payment.Reference
	}
	metadata["status"] = string(payment.Status)

	provider := payment.ConnectorID
	if payment.Provider != nil {
		provider = string(*payment.Provider)
	}

	return &models.Transaction{
		ID:         uuid.New(),
		Side:       models.TransactionSidePayments,
		Provider:   provider,
		ExternalID: payment.ID,
		Amount:     amount,
		Currency:   payment.Asset,
		OccurredAt: payment.CreatedAt,
		IngestedAt: time.Now(),
		Metadata:   metadata,
	}
}

func (e *BackfillExecutor) ingestTransaction(ctx context.Context, tx *models.Transaction) error {
	// Check for duplicates
	existing, err := e.txStore.ExistsByExternalIDs(ctx, tx.Side, []string{tx.ExternalID})
	if err != nil {
		return fmt.Errorf("failed to check existing: %w", err)
	}
	if existing[tx.ExternalID] {
		return nil // Already exists, skip
	}

	// Store in OpenSearch
	if err := e.txStore.Create(ctx, tx); err != nil {
		return fmt.Errorf("failed to create transaction: %w", err)
	}

	// Publish TRANSACTION_INGESTED event
	e.publishTransactionIngested(ctx, tx)

	return nil
}

func (e *BackfillExecutor) publishTransactionIngested(ctx context.Context, tx *models.Transaction) {
	if e.publisher == nil || e.topic == "" {
		return
	}

	event := events.NewTransactionIngestedEvent(tx)
	data, err := event.Marshal()
	if err != nil {
		logging.FromContext(ctx).Errorf("failed to marshal TRANSACTION_INGESTED event: %v", err)
		return
	}

	msg := message.NewMessage(uuid.New().String(), data)
	msg.SetContext(ctx)

	if err := e.publisher.Publish(e.topic, msg); err != nil {
		logging.FromContext(ctx).Errorf("failed to publish TRANSACTION_INGESTED event: %v", err)
	}
}

func int64Ptr(v int64) *int64 {
	return &v
}

// Ensure big.Int is used (referenced by SDK types)
var _ = (*big.Int)(nil)
