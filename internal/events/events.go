package events

import (
	"encoding/json"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
)

const (
	// AppName is the application name used in event envelopes.
	AppName = "reconciliation"

	// EventVersion is the current event format version.
	EventVersion = "v1"

	// EventTypeTransactionIngested is emitted after a transaction is successfully ingested.
	EventTypeTransactionIngested = "TRANSACTION_INGESTED"

	// EventTypeBackfill is emitted to trigger a historical data backfill.
	EventTypeBackfill = "BACKFILL"
)

// Event is the standard Formance stack event envelope.
type Event struct {
	App     string      `json:"app"`
	Version string      `json:"version"`
	Date    time.Time   `json:"date"`
	Type    string      `json:"type"`
	Payload interface{} `json:"payload"`
}

// TransactionIngestedPayload is the payload for TRANSACTION_INGESTED events.
type TransactionIngestedPayload struct {
	TransactionID string                 `json:"transactionId"`
	ExternalID    string                 `json:"externalId"`
	Side          string                 `json:"side"`
	Provider      string                 `json:"provider"`
	Amount        int64                  `json:"amount"`
	Currency      string                 `json:"currency"`
	OccurredAt    time.Time              `json:"occurredAt"`
	IngestedAt    time.Time              `json:"ingestedAt"`
	Metadata      map[string]interface{} `json:"metadata"`
}

// NewTransactionIngestedEvent creates a new TRANSACTION_INGESTED event from a transaction.
func NewTransactionIngestedEvent(tx *models.Transaction) Event {
	return Event{
		App:     AppName,
		Version: EventVersion,
		Date:    time.Now().UTC(),
		Type:    EventTypeTransactionIngested,
		Payload: TransactionIngestedPayload{
			TransactionID: tx.ID.String(),
			ExternalID:    tx.ExternalID,
			Side:          string(tx.Side),
			Provider:      tx.Provider,
			Amount:        tx.Amount,
			Currency:      tx.Currency,
			OccurredAt:    tx.OccurredAt,
			IngestedAt:    tx.IngestedAt,
			Metadata:      tx.Metadata,
		},
	}
}

// BackfillPayload is the payload for BACKFILL events.
type BackfillPayload struct {
	BackfillID string `json:"backfillId"`
}

// NewBackfillEvent creates a new BACKFILL event.
func NewBackfillEvent(backfillID string) Event {
	return Event{
		App:     AppName,
		Version: EventVersion,
		Date:    time.Now().UTC(),
		Type:    EventTypeBackfill,
		Payload: BackfillPayload{
			BackfillID: backfillID,
		},
	}
}

// Marshal serializes an event to JSON.
func (e Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}
