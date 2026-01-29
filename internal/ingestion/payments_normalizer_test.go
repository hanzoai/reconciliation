package ingestion

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// JSON fixture for SAVED_PAYMENT event (equivalent to PAYMENT_CREATED)
const paymentCreatedEventJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "pay_abc123",
		"connector": "stripe",
		"amount": 10000,
		"asset": "USD/2",
		"status": "PENDING",
		"createdAt": "2024-01-15T10:30:00Z",
		"reference": "ref_001",
		"metadata": {
			"ledger_reference": "ledger_tx_001",
			"order_id": "order_xyz789"
		}
	}
}`

// JSON fixture for SAVED_PAYMENT event with CAPTURED status
const paymentCapturedEventJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "pay_captured_001",
		"connector": "adyen",
		"amount": 25000,
		"asset": "EUR",
		"status": "CAPTURED",
		"createdAt": "2024-01-15T11:00:00Z",
		"reference": "ref_captured",
		"metadata": {
			"ledger_reference": "ledger_tx_captured",
			"order_id": "order_captured_001"
		}
	}
}`

// JSON fixture for Adyen connector
const adyenConnectorEventJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "adyen_pay_001",
		"connector": "adyen",
		"amount": 5000,
		"asset": "GBP",
		"status": "AUTHORISED",
		"createdAt": "2024-01-15T12:00:00Z",
		"reference": "adyen_ref_001",
		"metadata": {}
	}
}`

// JSON fixture for Stripe connector
const stripeConnectorEventJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "pi_stripe_001",
		"connector": "stripe",
		"amount": 7500,
		"asset": "USD",
		"status": "SUCCEEDED",
		"createdAt": "2024-01-15T13:00:00Z",
		"reference": "stripe_ref_001",
		"metadata": {
			"customer_id": "cus_123"
		}
	}
}`

// JSON fixture for event with complete metadata
const paymentWithMetadataEventJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "pay_metadata_001",
		"connector": "stripe",
		"amount": 15000,
		"asset": "USD",
		"status": "PENDING",
		"createdAt": "2024-01-15T14:00:00Z",
		"reference": "ref_metadata",
		"metadata": {
			"ledger_reference": "ledger_ref_full",
			"order_id": "order_full_metadata",
			"description": "Test payment with full metadata",
			"custom_field": "custom_value",
			"user_id": "user_123"
		}
	}
}`

// JSON fixture for non-relevant event
const nonRelevantPaymentsEventJSON = `{
	"type": "CONNECTOR_RESET",
	"payload": {
		"connectorId": "stripe_connector_001"
	}
}`

// JSON fixture for malformed event (missing payment id)
const malformedPaymentMissingIDJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"connector": "stripe",
		"amount": 1000,
		"asset": "USD",
		"status": "PENDING",
		"createdAt": "2024-01-15T10:00:00Z"
	}
}`

// JSON fixture for malformed event (missing connector)
const malformedPaymentMissingConnectorJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "pay_no_connector",
		"amount": 1000,
		"asset": "USD",
		"status": "PENDING",
		"createdAt": "2024-01-15T10:00:00Z"
	}
}`

// JSON fixture for malformed event (missing amount)
const malformedPaymentMissingAmountJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "pay_no_amount",
		"connector": "stripe",
		"asset": "USD",
		"status": "PENDING",
		"createdAt": "2024-01-15T10:00:00Z"
	}
}`

// JSON fixture for malformed event (missing asset)
const malformedPaymentMissingAssetJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "pay_no_asset",
		"connector": "stripe",
		"amount": 1000,
		"status": "PENDING",
		"createdAt": "2024-01-15T10:00:00Z"
	}
}`

// JSON fixture for malformed event (missing createdAt)
const malformedPaymentMissingCreatedAtJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "pay_no_created_at",
		"connector": "stripe",
		"amount": 1000,
		"asset": "USD",
		"status": "PENDING"
	}
}`

// JSON fixture for malformed event (invalid createdAt format)
const malformedPaymentInvalidCreatedAtJSON = `{
	"type": "SAVED_PAYMENT",
	"payload": {
		"id": "pay_invalid_date",
		"connector": "stripe",
		"amount": 1000,
		"asset": "USD",
		"status": "PENDING",
		"createdAt": "invalid-date-format"
	}
}`

func parsePaymentsEventFromJSON(t *testing.T, jsonStr string) PaymentsEvent {
	var event PaymentsEvent
	err := json.Unmarshal([]byte(jsonStr), &event)
	require.NoError(t, err, "Failed to parse JSON fixture")
	return event
}

func TestNormalizePaymentsEvent_PaymentCreated(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, paymentCreatedEventJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)

	assert.Equal(t, "pay_abc123", tx.ExternalID)
	assert.Equal(t, int64(10000), tx.Amount)
	assert.Equal(t, "USD/2", tx.Currency)
	assert.Equal(t, models.TransactionSidePayments, tx.Side)
	assert.Equal(t, "stripe", tx.Provider)
	assert.Equal(t, "2024-01-15T10:30:00Z", tx.OccurredAt.Format("2006-01-02T15:04:05Z"))
	assert.NotEmpty(t, tx.ID)
	assert.NotZero(t, tx.IngestedAt)

	// Check metadata
	assert.Equal(t, "ledger_tx_001", tx.Metadata["ledger_reference"])
	assert.Equal(t, "order_xyz789", tx.Metadata["order_id"])
	assert.Equal(t, "PENDING", tx.Metadata["status"])
	assert.Equal(t, "ref_001", tx.Metadata["reference"])
}

func TestNormalizePaymentsEvent_PaymentCaptured(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, paymentCapturedEventJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)

	assert.Equal(t, "pay_captured_001", tx.ExternalID)
	assert.Equal(t, int64(25000), tx.Amount)
	assert.Equal(t, "EUR", tx.Currency)
	assert.Equal(t, "adyen", tx.Provider)
	assert.Equal(t, "CAPTURED", tx.Metadata["status"])
	assert.Equal(t, "ledger_tx_captured", tx.Metadata["ledger_reference"])
	assert.Equal(t, "order_captured_001", tx.Metadata["order_id"])
}

func TestNormalizePaymentsEvent_AdyenConnector(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, adyenConnectorEventJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)

	assert.Equal(t, "adyen_pay_001", tx.ExternalID)
	assert.Equal(t, "adyen", tx.Provider)
	assert.Equal(t, int64(5000), tx.Amount)
	assert.Equal(t, "GBP", tx.Currency)
	assert.Equal(t, "AUTHORISED", tx.Metadata["status"])
}

func TestNormalizePaymentsEvent_StripeConnector(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, stripeConnectorEventJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)

	assert.Equal(t, "pi_stripe_001", tx.ExternalID)
	assert.Equal(t, "stripe", tx.Provider)
	assert.Equal(t, int64(7500), tx.Amount)
	assert.Equal(t, "USD", tx.Currency)
	assert.Equal(t, "SUCCEEDED", tx.Metadata["status"])
	assert.Equal(t, "cus_123", tx.Metadata["customer_id"])
}

func TestNormalizePaymentsEvent_WithMetadata(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, paymentWithMetadataEventJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)

	// Verify metadata extraction
	assert.Equal(t, "ledger_ref_full", tx.Metadata["ledger_reference"])
	assert.Equal(t, "order_full_metadata", tx.Metadata["order_id"])
	assert.Equal(t, "Test payment with full metadata", tx.Metadata["description"])
	assert.Equal(t, "custom_value", tx.Metadata["custom_field"])
	assert.Equal(t, "user_123", tx.Metadata["user_id"])
	assert.Equal(t, "ref_metadata", tx.Metadata["reference"])
	assert.Equal(t, "PENDING", tx.Metadata["status"])
}

func TestNormalizePaymentsEvent_NonRelevantEvent_ReturnsNilNil(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, nonRelevantPaymentsEventJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.NoError(t, err)
	assert.Nil(t, tx)
}

func TestNormalizePaymentsEvent_MalformedEvent_MissingID(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, malformedPaymentMissingIDJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing payment id")
}

func TestNormalizePaymentsEvent_MalformedEvent_MissingConnector(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, malformedPaymentMissingConnectorJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing provider, connectorID, or connector")
}

func TestNormalizePaymentsEvent_MalformedEvent_MissingAmount(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, malformedPaymentMissingAmountJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing amount")
}

func TestNormalizePaymentsEvent_MalformedEvent_MissingAsset(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, malformedPaymentMissingAssetJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing asset")
}

func TestNormalizePaymentsEvent_MalformedEvent_MissingCreatedAt(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, malformedPaymentMissingCreatedAtJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing createdAt")
}

func TestNormalizePaymentsEvent_MalformedEvent_InvalidCreatedAt(t *testing.T) {
	ctx := context.Background()
	event := parsePaymentsEventFromJSON(t, malformedPaymentInvalidCreatedAtJSON)

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "invalid createdAt format")
}

func TestNormalizePaymentsEvent_EmptyPayload(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type:    PaymentsEventTypeSavedPayment,
		Payload: nil,
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "payload is nil")
}

func TestNormalizePaymentsEvent_EmptyPaymentID(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "",
			"connector": "stripe",
			"amount":    float64(1000),
			"asset":     "USD",
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "payment id is empty")
}

func TestNormalizePaymentsEvent_EmptyConnector(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_001",
			"connector": "",
			"amount":    float64(1000),
			"asset":     "USD",
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing provider, connectorID, or connector")
}

func TestNormalizePaymentsEvent_EmptyAsset(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_001",
			"connector": "stripe",
			"amount":    float64(1000),
			"asset":     "",
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "asset is empty")
}

func TestNormalizePaymentsEvent_RFC3339NanoTimestamp(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_nano_001",
			"connector": "stripe",
			"amount":    float64(1000),
			"asset":     "USD",
			"createdAt": "2024-01-15T10:30:00.123456789Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, 2024, tx.OccurredAt.Year())
	assert.Equal(t, 1, int(tx.OccurredAt.Month()))
	assert.Equal(t, 15, tx.OccurredAt.Day())
}

func TestNormalizePaymentsEvent_StringAmount(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_string_amount",
			"connector": "stripe",
			"amount":    "12345",
			"asset":     "USD",
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)
	assert.Equal(t, int64(12345), tx.Amount)
}

func TestNormalizePaymentsEvent_InvalidAmountString(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_invalid_amount",
			"connector": "stripe",
			"amount":    "not_a_number",
			"asset":     "USD",
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "invalid amount string")
}

func TestNormalizePaymentsEvent_UnsupportedAmountType(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_bad_amount_type",
			"connector": "stripe",
			"amount":    struct{}{},
			"asset":     "USD",
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "unsupported amount type")
}

func TestNormalizePaymentsEvent_NoMetadata(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_no_metadata",
			"connector": "stripe",
			"amount":    float64(1000),
			"asset":     "USD",
			"status":    "PENDING",
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	require.NoError(t, err)
	require.NotNil(t, tx)
	// Status should still be extracted
	assert.Equal(t, "PENDING", tx.Metadata["status"])
}

func TestNormalizePaymentsEvent_InvalidPaymentIDType(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        12345, // should be string
			"connector": "stripe",
			"amount":    float64(1000),
			"asset":     "USD",
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "payment id must be a string")
}

func TestNormalizePaymentsEvent_InvalidConnectorType(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_001",
			"connector": 12345, // should be string
			"amount":    float64(1000),
			"asset":     "USD",
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "missing provider, connectorID, or connector")
}

func TestNormalizePaymentsEvent_InvalidAssetType(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_001",
			"connector": "stripe",
			"amount":    float64(1000),
			"asset":     12345, // should be string
			"createdAt": "2024-01-15T10:00:00Z",
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "asset must be a string")
}

func TestNormalizePaymentsEvent_InvalidCreatedAtType(t *testing.T) {
	ctx := context.Background()
	event := PaymentsEvent{
		Type: PaymentsEventTypeSavedPayment,
		Payload: map[string]interface{}{
			"id":        "pay_001",
			"connector": "stripe",
			"amount":    float64(1000),
			"asset":     "USD",
			"createdAt": 12345, // should be string or time.Time
		},
	}

	tx, err := NormalizePaymentsEvent(ctx, event)

	assert.Error(t, err)
	assert.Nil(t, tx)
	assert.Contains(t, err.Error(), "unsupported createdAt type")
}
