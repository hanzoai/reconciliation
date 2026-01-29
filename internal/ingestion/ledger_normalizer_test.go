package ingestion

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// JSON fixture for TRANSACTION_CREATED event
const transactionCreatedEventJSON = `{
	"type": "COMMITTED_TRANSACTIONS",
	"ledger": "default",
	"payload": {
		"transaction": {
			"id": "12345",
			"timestamp": "2024-01-15T10:30:00Z",
			"postings": [
				{
					"source": "world",
					"destination": "users:123",
					"amount": 1000,
					"asset": "USD/2"
				}
			],
			"metadata": {
				"payment_id": "pay_abc123",
				"order_id": "order_xyz789"
			}
		}
	}
}`

// JSON fixture for event with multiple postings
const multiplePostingsEventJSON = `{
	"type": "COMMITTED_TRANSACTIONS",
	"ledger": "payments",
	"payload": {
		"transaction": {
			"id": "67890",
			"timestamp": "2024-01-15T11:00:00Z",
			"postings": [
				{
					"source": "world",
					"destination": "users:123",
					"amount": 500,
					"asset": "EUR"
				},
				{
					"source": "world",
					"destination": "fees:platform",
					"amount": 50,
					"asset": "EUR"
				},
				{
					"source": "world",
					"destination": "taxes:vat",
					"amount": 25,
					"asset": "EUR"
				}
			],
			"metadata": {}
		}
	}
}`

// JSON fixture for event with complete metadata
const completeMetadataEventJSON = `{
	"type": "COMMITTED_TRANSACTIONS",
	"ledger": "main-ledger",
	"payload": {
		"transaction": {
			"id": "txn_complete_001",
			"timestamp": "2024-01-15T12:00:00Z",
			"postings": [
				{
					"source": "bank:main",
					"destination": "users:456",
					"amount": 2500,
					"asset": "GBP"
				}
			],
			"metadata": {
				"payment_id": "pay_full_metadata",
				"order_id": "order_full_metadata",
				"reference": "REF-2024-001",
				"description": "Test transaction with full metadata",
				"custom_field": "custom_value"
			}
		}
	}
}`

// JSON fixture for non-relevant event (ACCOUNT_CREATED)
const accountCreatedEventJSON = `{
	"type": "SAVED_METADATA",
	"ledger": "default",
	"payload": {
		"targetType": "ACCOUNT",
		"targetId": "users:789",
		"metadata": {
			"name": "Test User"
		}
	}
}`

// JSON fixture for malformed event (missing postings)
const malformedEventMissingPostingsJSON = `{
	"type": "COMMITTED_TRANSACTIONS",
	"ledger": "default",
	"payload": {
		"transaction": {
			"id": "bad_txn_001",
			"timestamp": "2024-01-15T13:00:00Z"
		}
	}
}`

// JSON fixture for malformed event (missing timestamp)
const malformedEventMissingTimestampJSON = `{
	"type": "COMMITTED_TRANSACTIONS",
	"ledger": "default",
	"payload": {
		"transaction": {
			"id": "bad_txn_002",
			"postings": [
				{
					"source": "world",
					"destination": "users:123",
					"amount": 100,
					"asset": "USD"
				}
			]
		}
	}
}`

// JSON fixture for malformed event (missing transaction id)
const malformedEventMissingIDJSON = `{
	"type": "COMMITTED_TRANSACTIONS",
	"ledger": "default",
	"payload": {
		"transaction": {
			"timestamp": "2024-01-15T13:00:00Z",
			"postings": [
				{
					"source": "world",
					"destination": "users:123",
					"amount": 100,
					"asset": "USD"
				}
			]
		}
	}
}`

// JSON fixture for event with mixed currencies (should error)
const mixedCurrenciesEventJSON = `{
	"type": "COMMITTED_TRANSACTIONS",
	"ledger": "default",
	"payload": {
		"transaction": {
			"id": "mixed_currency_001",
			"timestamp": "2024-01-15T14:00:00Z",
			"postings": [
				{
					"source": "world",
					"destination": "users:123",
					"amount": 100,
					"asset": "USD"
				},
				{
					"source": "world",
					"destination": "users:456",
					"amount": 200,
					"asset": "EUR"
				}
			]
		}
	}
}`

func parseEventFromJSON(t *testing.T, jsonStr string) LedgerEvent {
	var event LedgerEvent
	err := json.Unmarshal([]byte(jsonStr), &event)
	require.NoError(t, err, "Failed to parse JSON fixture")
	return event
}

func TestNormalizeLedgerEvent_TransactionCreated(t *testing.T) {
	ctx := context.Background()
	event := parseEventFromJSON(t, transactionCreatedEventJSON)

	tx, err := NormalizeLedgerEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)

	assert.Equal(t, "12345", tx.ExternalID)
	assert.Equal(t, int64(1000), tx.Amount)
	assert.Equal(t, "USD/2", tx.Currency)
	assert.Equal(t, models.TransactionSideLedger, tx.Side)
	assert.Equal(t, "default", tx.Provider)
	assert.Equal(t, "2024-01-15T10:30:00Z", tx.OccurredAt.Format("2006-01-02T15:04:05Z"))
	assert.NotEmpty(t, tx.ID)
	assert.NotZero(t, tx.IngestedAt)
}

func TestNormalizeLedgerEvent_MultiplePostings_AmountAggregated(t *testing.T) {
	ctx := context.Background()
	event := parseEventFromJSON(t, multiplePostingsEventJSON)

	tx, err := NormalizeLedgerEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)

	// Amount should be aggregated: 500 + 50 + 25 = 575
	assert.Equal(t, int64(575), tx.Amount)
	assert.Equal(t, "EUR", tx.Currency)
	assert.Equal(t, "67890", tx.ExternalID)
	assert.Equal(t, "payments", tx.Provider)
}

func TestNormalizeLedgerEvent_CompleteMetadata(t *testing.T) {
	ctx := context.Background()
	event := parseEventFromJSON(t, completeMetadataEventJSON)

	tx, err := NormalizeLedgerEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)

	// Verify metadata extraction
	assert.Equal(t, "pay_full_metadata", tx.Metadata["payment_id"])
	assert.Equal(t, "order_full_metadata", tx.Metadata["order_id"])
	assert.Equal(t, "REF-2024-001", tx.Metadata["reference"])
	assert.Equal(t, "Test transaction with full metadata", tx.Metadata["description"])
	assert.Equal(t, "custom_value", tx.Metadata["custom_field"])

	// Verify other fields
	assert.Equal(t, "txn_complete_001", tx.ExternalID)
	assert.Equal(t, int64(2500), tx.Amount)
	assert.Equal(t, "GBP", tx.Currency)
	assert.Equal(t, "main-ledger", tx.Provider)
}

func TestNormalizeLedgerEvent_NonRelevantEvent_ReturnsNilNil(t *testing.T) {
	ctx := context.Background()
	event := parseEventFromJSON(t, accountCreatedEventJSON)

	tx, err := NormalizeLedgerEvent(ctx, event)

	assert.NoError(t, err)
	assert.Nil(t, tx)
}

func TestNormalizeLedgerEvent_MalformedEvent_MissingPostings(t *testing.T) {
	ctx := context.Background()
	event := parseEventFromJSON(t, malformedEventMissingPostingsJSON)

	tx, err := NormalizeLedgerEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing postings")
}

func TestNormalizeLedgerEvent_MalformedEvent_MissingTimestamp(t *testing.T) {
	ctx := context.Background()
	event := parseEventFromJSON(t, malformedEventMissingTimestampJSON)

	tx, err := NormalizeLedgerEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing timestamp")
}

func TestNormalizeLedgerEvent_MalformedEvent_MissingID(t *testing.T) {
	ctx := context.Background()
	event := parseEventFromJSON(t, malformedEventMissingIDJSON)

	tx, err := NormalizeLedgerEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing transaction id")
}

func TestNormalizeLedgerEvent_MixedCurrencies_ReturnsError(t *testing.T) {
	ctx := context.Background()
	event := parseEventFromJSON(t, mixedCurrenciesEventJSON)

	tx, err := NormalizeLedgerEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "mixed currencies")
}

func TestNormalizeLedgerEvent_EmptyPayload(t *testing.T) {
	ctx := context.Background()
	event := LedgerEvent{
		Type:    LedgerEventTypeTransactionCreated,
		Ledger:  "default",
		Payload: nil,
	}

	tx, err := NormalizeLedgerEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "payload is nil")
}

func TestNormalizeLedgerEvent_EmptyPostingsArray(t *testing.T) {
	ctx := context.Background()
	event := LedgerEvent{
		Type:   LedgerEventTypeTransactionCreated,
		Ledger: "default",
		Payload: map[string]interface{}{
			"transaction": map[string]interface{}{
				"id":        "empty_postings",
				"timestamp": "2024-01-15T10:00:00Z",
				"postings":  []interface{}{},
			},
		},
	}

	tx, err := NormalizeLedgerEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "postings array is empty")
}

func TestNormalizeLedgerEvent_NumericTransactionID(t *testing.T) {
	ctx := context.Background()
	event := LedgerEvent{
		Type:   LedgerEventTypeTransactionCreated,
		Ledger: "default",
		Payload: map[string]interface{}{
			"transaction": map[string]interface{}{
				"id":        float64(98765), // JSON numbers are parsed as float64
				"timestamp": "2024-01-15T10:00:00Z",
				"postings": []interface{}{
					map[string]interface{}{
						"source":      "world",
						"destination": "users:123",
						"amount":      float64(100),
						"asset":       "USD",
					},
				},
			},
		},
	}

	tx, err := NormalizeLedgerEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, "98765", tx.ExternalID)
}

func TestNormalizeLedgerEvent_StringAmount(t *testing.T) {
	ctx := context.Background()
	event := LedgerEvent{
		Type:   LedgerEventTypeTransactionCreated,
		Ledger: "default",
		Payload: map[string]interface{}{
			"transaction": map[string]interface{}{
				"id":        "string_amount_001",
				"timestamp": "2024-01-15T10:00:00Z",
				"postings": []interface{}{
					map[string]interface{}{
						"source":      "world",
						"destination": "users:123",
						"amount":      "12345", // Amount as string
						"asset":       "USD",
					},
				},
			},
		},
	}

	tx, err := NormalizeLedgerEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, int64(12345), tx.Amount)
}

func TestNormalizeLedgerEvent_RFC3339NanoTimestamp(t *testing.T) {
	ctx := context.Background()
	event := LedgerEvent{
		Type:   LedgerEventTypeTransactionCreated,
		Ledger: "default",
		Payload: map[string]interface{}{
			"transaction": map[string]interface{}{
				"id":        "nano_timestamp_001",
				"timestamp": "2024-01-15T10:30:00.123456789Z",
				"postings": []interface{}{
					map[string]interface{}{
						"source":      "world",
						"destination": "users:123",
						"amount":      float64(100),
						"asset":       "USD",
					},
				},
			},
		},
	}

	tx, err := NormalizeLedgerEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, 2024, tx.OccurredAt.Year())
	assert.Equal(t, 1, int(tx.OccurredAt.Month()))
	assert.Equal(t, 15, tx.OccurredAt.Day())
}

func TestNormalizeLedgerEvent_PayloadWithoutTransactionWrapper(t *testing.T) {
	ctx := context.Background()
	// Some event formats may have the transaction data directly in payload
	event := LedgerEvent{
		Type:   LedgerEventTypeTransactionCreated,
		Ledger: "default",
		Payload: map[string]interface{}{
			"id":        "direct_payload_001",
			"timestamp": "2024-01-15T10:00:00Z",
			"postings": []interface{}{
				map[string]interface{}{
					"source":      "world",
					"destination": "users:123",
					"amount":      float64(100),
					"asset":       "USD",
				},
			},
		},
	}

	tx, err := NormalizeLedgerEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, "direct_payload_001", tx.ExternalID)
}

func TestExtractPostingAmount_InvalidString(t *testing.T) {
	posting := map[string]interface{}{
		"amount": "not_a_number",
	}

	amount, err := extractPostingAmount(posting)

	assert.Error(t, err)
	assert.Equal(t, int64(0), amount)
	assert.Contains(t, err.Error(), "invalid amount string")
}

func TestExtractPostingAmount_UnsupportedType(t *testing.T) {
	posting := map[string]interface{}{
		"amount": struct{}{},
	}

	amount, err := extractPostingAmount(posting)

	assert.Error(t, err)
	assert.Equal(t, int64(0), amount)
	assert.Contains(t, err.Error(), "unsupported amount type")
}

// TestNormalizeLedgerEvent_TransactionsArrayFormat tests the "transactions" (plural) array format
// This is the format used by the reconcile-test seeder tool.
func TestNormalizeLedgerEvent_TransactionsArrayFormat(t *testing.T) {
	ctx := context.Background()

	// Use a timestamp 25 hours in the past (similar to orphan detection test)
	pastTimestamp := "2024-01-14T09:00:00Z" // 25 hours before 2024-01-15T10:00:00Z

	event := LedgerEvent{
		Type:   LedgerEventTypeTransactionCreated,
		Ledger: "test-ledger",
		Payload: map[string]interface{}{
			"transactions": []interface{}{
				map[string]interface{}{
					"id":        "orphan-ledger-0",
					"timestamp": pastTimestamp,
					"postings": []interface{}{
						map[string]interface{}{
							"source":      "world",
							"destination": "users:test",
							"amount":      float64(750),
							"asset":       "USD/2",
						},
					},
					"metadata": map[string]interface{}{
						"test_scenario": "orphan-ledger",
					},
				},
			},
		},
	}

	tx, err := NormalizeLedgerEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)

	// Verify the transaction is correctly normalized
	assert.Equal(t, "orphan-ledger-0", tx.ExternalID)
	assert.Equal(t, int64(750), tx.Amount)
	assert.Equal(t, "USD/2", tx.Currency)
	assert.Equal(t, models.TransactionSideLedger, tx.Side)
	assert.Equal(t, "test-ledger", tx.Provider)

	// CRITICAL: Verify the timestamp is correctly extracted (not overwritten with current time)
	assert.Equal(t, 2024, tx.OccurredAt.Year())
	assert.Equal(t, 1, int(tx.OccurredAt.Month()))
	assert.Equal(t, 14, tx.OccurredAt.Day())
	assert.Equal(t, 9, tx.OccurredAt.Hour())

	// Verify metadata
	assert.Equal(t, "orphan-ledger", tx.Metadata["test_scenario"])
}

// TestNormalizeLedgerEvent_TransactionsArrayFormat_MultipleTransactions tests that batch
// transactions are rejected with an explicit error.
func TestNormalizeLedgerEvent_TransactionsArrayFormat_MultipleTransactions(t *testing.T) {
	ctx := context.Background()

	event := LedgerEvent{
		Type:   LedgerEventTypeTransactionCreated,
		Ledger: "test-ledger",
		Payload: map[string]interface{}{
			"transactions": []interface{}{
				map[string]interface{}{
					"id":        "tx-first",
					"timestamp": "2024-01-15T10:00:00Z",
					"postings": []interface{}{
						map[string]interface{}{
							"source":      "world",
							"destination": "users:123",
							"amount":      float64(100),
							"asset":       "USD",
						},
					},
				},
				map[string]interface{}{
					"id":        "tx-second",
					"timestamp": "2024-01-15T11:00:00Z",
					"postings": []interface{}{
						map[string]interface{}{
							"source":      "world",
							"destination": "users:456",
							"amount":      float64(200),
							"asset":       "USD",
						},
					},
				},
			},
		},
	}

	tx, err := NormalizeLedgerEvent(ctx, event)

	require.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "batch transactions are not supported")
}
