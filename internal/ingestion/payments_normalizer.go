package ingestion

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
)

const (
	// PaymentsEventTypeSavedPayment is the event type for payment save operations.
	PaymentsEventTypeSavedPayment = "SAVED_PAYMENT"
)

// PaymentsEvent represents a raw event from the Payments system.
type PaymentsEvent struct {
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

// NormalizePaymentsEvent transforms a raw Payments event into a Transaction struct.
// Returns (nil, nil) for non-relevant events that should be skipped.
// Returns (*Transaction, nil) for successfully normalized transactions.
// Returns (nil, error) for malformed events that cannot be processed.
func NormalizePaymentsEvent(ctx context.Context, event PaymentsEvent) (*models.Transaction, error) {
	// Check if this is a payment event we care about
	if event.Type != PaymentsEventTypeSavedPayment {
		logging.FromContext(ctx).Debugf("Skipping non-relevant payments event type: %s", event.Type)
		return nil, nil
	}

	logging.FromContext(ctx).Debugf("Processing payments event: type=%s", event.Type)

	// Validate payload exists
	if event.Payload == nil {
		return nil, fmt.Errorf("payload is nil")
	}

	// Extract external ID from payment_id (the "id" field in payload)
	externalID, err := extractPaymentID(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to extract payment_id: %w", err)
	}

	// Extract provider (supports v3.0.0 and legacy formats)
	provider, err := extractProvider(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to extract provider: %w", err)
	}

	// Extract amount
	amount, err := extractPaymentAmount(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to extract amount: %w", err)
	}

	// Extract currency from asset
	currency, err := extractAsset(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to extract asset: %w", err)
	}

	// Extract occurred_at timestamp from createdAt
	occurredAt, err := extractCreatedAt(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to extract createdAt: %w", err)
	}

	// Extract metadata
	metadata := extractPaymentMetadata(event.Payload)

	tx := &models.Transaction{
		ID:         uuid.New(),
		Side:       models.TransactionSidePayments,
		Provider:   provider,
		ExternalID: externalID,
		Amount:     amount,
		Currency:   currency,
		OccurredAt: occurredAt,
		IngestedAt: time.Now(),
		Metadata:   metadata,
	}

	logging.FromContext(ctx).WithFields(map[string]interface{}{
		"external_id": tx.ExternalID,
		"provider":    tx.Provider,
		"occurred_at": tx.OccurredAt,
	}).Debug("Created payment transaction")

	return tx, nil
}

// extractPaymentID extracts the payment ID from the payload.
func extractPaymentID(payload map[string]interface{}) (string, error) {
	id, ok := payload["id"]
	if !ok {
		return "", fmt.Errorf("missing payment id")
	}

	switch v := id.(type) {
	case string:
		if v == "" {
			return "", fmt.Errorf("payment id is empty")
		}
		return v, nil
	default:
		return "", fmt.Errorf("payment id must be a string, got %T", id)
	}
}

// extractProvider extracts the provider from the payload.
// Supports both v3.0.0 format (provider field) and legacy format (connector field).
func extractProvider(payload map[string]interface{}) (string, error) {
	// Try "provider" first (v3.0.0 format)
	if provider, ok := payload["provider"]; ok {
		if v, ok := provider.(string); ok && v != "" {
			return v, nil
		}
	}

	// Try "connectorID" (v3.0.0 format)
	if connectorID, ok := payload["connectorID"]; ok {
		if v, ok := connectorID.(string); ok && v != "" {
			return v, nil
		}
	}

	// Fallback to "connector" (legacy format)
	if connector, ok := payload["connector"]; ok {
		if v, ok := connector.(string); ok && v != "" {
			return v, nil
		}
	}

	return "", fmt.Errorf("missing provider, connectorID, or connector")
}

// extractPaymentAmount extracts the amount from the payload.
func extractPaymentAmount(payload map[string]interface{}) (int64, error) {
	amountRaw, ok := payload["amount"]
	if !ok {
		return 0, fmt.Errorf("missing amount")
	}

	switch v := amountRaw.(type) {
	case float64:
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("amount has fractional part: %v", v)
		}
		if v > float64(math.MaxInt64) || v < float64(math.MinInt64) {
			return 0, fmt.Errorf("amount overflows int64: %v", v)
		}
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case string:
		var amount int64
		_, err := fmt.Sscanf(v, "%d", &amount)
		if err != nil {
			return 0, fmt.Errorf("invalid amount string: %s", v)
		}
		return amount, nil
	default:
		return 0, fmt.Errorf("unsupported amount type: %T", amountRaw)
	}
}

// extractAsset extracts the asset (currency) from the payload.
func extractAsset(payload map[string]interface{}) (string, error) {
	asset, ok := payload["asset"]
	if !ok {
		return "", fmt.Errorf("missing asset")
	}

	switch v := asset.(type) {
	case string:
		if v == "" {
			return "", fmt.Errorf("asset is empty")
		}
		return v, nil
	default:
		return "", fmt.Errorf("asset must be a string, got %T", asset)
	}
}

// extractCreatedAt extracts the createdAt timestamp from the payload.
func extractCreatedAt(payload map[string]interface{}) (time.Time, error) {
	createdAt, ok := payload["createdAt"]
	if !ok {
		return time.Time{}, fmt.Errorf("missing createdAt")
	}

	switch v := createdAt.(type) {
	case string:
		// Try RFC3339 format first
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, nil
		}
		// Try RFC3339Nano format
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t, nil
		}
		return time.Time{}, fmt.Errorf("invalid createdAt format: %s", v)
	case time.Time:
		return v, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported createdAt type: %T", createdAt)
	}
}

// extractPaymentMetadata extracts metadata from the payment payload.
// It specifically looks for ledger_reference and order_id in the metadata.
func extractPaymentMetadata(payload map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	// Extract status if present (useful for reconciliation)
	if status, ok := payload["status"].(string); ok {
		result["status"] = status
	}

	// Extract reference if present
	if reference, ok := payload["reference"].(string); ok {
		result["reference"] = reference
	}

	metadataRaw, ok := payload["metadata"]
	if !ok {
		return result
	}

	metadata, ok := metadataRaw.(map[string]interface{})
	if !ok {
		return result
	}

	// Copy relevant metadata fields
	relevantFields := []string{"ledger_reference", "order_id", "description"}
	for _, field := range relevantFields {
		if val, exists := metadata[field]; exists {
			result[field] = val
		}
	}

	// Also include any other metadata fields
	for key, val := range metadata {
		if _, exists := result[key]; !exists {
			result[key] = val
		}
	}

	return result
}
