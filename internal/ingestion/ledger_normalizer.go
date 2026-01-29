package ingestion

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
)

const (
	// LedgerEventTypeTransactionCreated is the event type for transaction creation.
	LedgerEventTypeTransactionCreated = "COMMITTED_TRANSACTIONS"
)

// LedgerEvent represents a raw event from the Ledger system.
type LedgerEvent struct {
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
	Ledger  string                 `json:"ledger"`
}

// LedgerPosting represents a posting in a ledger transaction.
type LedgerPosting struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Amount      *big.Int `json:"amount"`
	Asset       string   `json:"asset"`
}

// NormalizeLedgerEvent transforms a raw Ledger event into Transaction structs.
// Returns (nil, nil) for non-relevant events that should be skipped.
// Returns ([]*Transaction, nil) for successfully normalized transactions.
// Returns (nil, error) for malformed events that cannot be processed.
func NormalizeLedgerEvent(ctx context.Context, event LedgerEvent) (*models.Transaction, error) {
	// Check if this is a transaction event we care about
	if event.Type != LedgerEventTypeTransactionCreated {
		logging.FromContext(ctx).Debugf("Skipping non-relevant ledger event type: %s", event.Type)
		return nil, nil
	}

	fmt.Printf("[DEBUG-NORMALIZE-LEDGER] Processing event: type=%s, ledger=%s\n", event.Type, event.Ledger)

	// Extract transaction data from payload (supports both single and batch formats)
	txDataList, err := extractTransactionDataList(event.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to extract transaction data: %w", err)
	}

	if len(txDataList) == 0 {
		return nil, nil
	}

	// For now, process only the first transaction
	// TODO: Support batch processing for multiple transactions
	txData := txDataList[0]

	// Debug: show raw transaction data
	fmt.Printf("[DEBUG-NORMALIZE-LEDGER] Raw txData keys: ")
	for k := range txData {
		fmt.Printf("%s, ", k)
	}
	fmt.Println()
	if ts, ok := txData["timestamp"]; ok {
		fmt.Printf("[DEBUG-NORMALIZE-LEDGER] Raw timestamp value: %v (type: %T)\n", ts, ts)
	} else {
		fmt.Println("[DEBUG-NORMALIZE-LEDGER] WARNING: No 'timestamp' field in txData!")
	}

	// Extract external ID from transaction_id
	externalID, err := extractExternalID(txData)
	if err != nil {
		return nil, fmt.Errorf("failed to extract external_id: %w", err)
	}

	// Extract and aggregate amount from postings
	amount, currency, err := extractAmountFromPostings(txData)
	if err != nil {
		return nil, fmt.Errorf("failed to extract amount from postings: %w", err)
	}

	// Extract occurred_at timestamp
	occurredAt, err := extractOccurredAt(txData)
	if err != nil {
		return nil, fmt.Errorf("failed to extract occurred_at: %w", err)
	}

	// Extract metadata
	metadata := extractMetadata(txData)

	tx := &models.Transaction{
		ID:         uuid.New(),
		Side:       models.TransactionSideLedger,
		Provider:   event.Ledger,
		ExternalID: externalID,
		Amount:     amount,
		Currency:   currency,
		OccurredAt: occurredAt,
		IngestedAt: time.Now(),
		Metadata:   metadata,
	}

	fmt.Printf("[DEBUG-NORMALIZE-LEDGER] Created transaction: externalID=%s, provider=%s, occurredAt=%v\n",
		tx.ExternalID, tx.Provider, tx.OccurredAt)

	return tx, nil
}

// extractTransactionDataList extracts transaction objects from the payload.
// Supports multiple formats:
// - payload.transactions (array) - Ledger v2.0.0 COMMITTED_TRANSACTIONS format
// - payload.transaction (object) - Legacy single transaction format
// - payload itself if it has an "id" field
func extractTransactionDataList(payload map[string]interface{}) ([]map[string]interface{}, error) {
	if payload == nil {
		return nil, fmt.Errorf("payload is nil")
	}

	// Try "transactions" (plural) - Ledger v2.0.0 format
	if txsRaw, ok := payload["transactions"]; ok {
		txsArray, ok := txsRaw.([]interface{})
		if !ok {
			return nil, fmt.Errorf("transactions field is not an array")
		}

		result := make([]map[string]interface{}, 0, len(txsArray))
		for i, txRaw := range txsArray {
			txData, ok := txRaw.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("transaction %d is not an object", i)
			}
			result = append(result, txData)
		}
		return result, nil
	}

	// Try "transaction" (singular) - Legacy format
	if txData, ok := payload["transaction"].(map[string]interface{}); ok {
		return []map[string]interface{}{txData}, nil
	}

	// If payload itself has an "id" field, treat it as the transaction
	if _, hasID := payload["id"]; hasID {
		return []map[string]interface{}{payload}, nil
	}

	return nil, fmt.Errorf("missing transaction data in payload (expected 'transactions' array or 'transaction' object)")
}

// extractExternalID extracts the external ID from the transaction data.
func extractExternalID(txData map[string]interface{}) (string, error) {
	// Try "id" field first (this is the transaction ID in Formance Ledger)
	if id, ok := txData["id"]; ok {
		switch v := id.(type) {
		case string:
			return v, nil
		case float64:
			return fmt.Sprintf("%.0f", v), nil
		case int:
			return fmt.Sprintf("%d", v), nil
		case int64:
			return fmt.Sprintf("%d", v), nil
		}
	}

	// Try "txid" field as fallback
	if txid, ok := txData["txid"]; ok {
		switch v := txid.(type) {
		case string:
			return v, nil
		case float64:
			return fmt.Sprintf("%.0f", v), nil
		case int:
			return fmt.Sprintf("%d", v), nil
		case int64:
			return fmt.Sprintf("%d", v), nil
		}
	}

	return "", fmt.Errorf("missing transaction id")
}

// extractAmountFromPostings extracts and aggregates the amount from postings.
// Returns the total amount as a positive value and the currency.
func extractAmountFromPostings(txData map[string]interface{}) (int64, string, error) {
	postingsRaw, ok := txData["postings"]
	if !ok {
		return 0, "", fmt.Errorf("missing postings in transaction data")
	}

	postings, ok := postingsRaw.([]interface{})
	if !ok {
		return 0, "", fmt.Errorf("postings is not an array")
	}

	if len(postings) == 0 {
		return 0, "", fmt.Errorf("postings array is empty")
	}

	var totalAmount int64
	var currency string

	for i, p := range postings {
		posting, ok := p.(map[string]interface{})
		if !ok {
			return 0, "", fmt.Errorf("posting %d is not an object", i)
		}

		// Extract amount
		amount, err := extractPostingAmount(posting)
		if err != nil {
			return 0, "", fmt.Errorf("posting %d: %w", i, err)
		}
		totalAmount += amount

		// Extract asset/currency
		asset, ok := posting["asset"].(string)
		if !ok {
			return 0, "", fmt.Errorf("posting %d: missing or invalid asset", i)
		}

		// Validate all postings use the same currency
		if currency == "" {
			currency = asset
		} else if currency != asset {
			return 0, "", fmt.Errorf("mixed currencies in postings: %s and %s", currency, asset)
		}
	}

	return totalAmount, currency, nil
}

// extractPostingAmount extracts the amount from a single posting.
func extractPostingAmount(posting map[string]interface{}) (int64, error) {
	amountRaw, ok := posting["amount"]
	if !ok {
		return 0, fmt.Errorf("missing amount")
	}

	switch v := amountRaw.(type) {
	case float64:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case *big.Int:
		return v.Int64(), nil
	case string:
		bi := new(big.Int)
		if _, ok := bi.SetString(v, 10); !ok {
			return 0, fmt.Errorf("invalid amount string: %s", v)
		}
		return bi.Int64(), nil
	default:
		return 0, fmt.Errorf("unsupported amount type: %T", amountRaw)
	}
}

// extractOccurredAt extracts the timestamp when the transaction occurred.
func extractOccurredAt(txData map[string]interface{}) (time.Time, error) {
	// Try "timestamp" field first
	if ts, ok := txData["timestamp"]; ok {
		result, err := parseTimestamp(ts)
		if err == nil {
			fmt.Printf("[DEBUG-NORMALIZER] Extracted timestamp: raw=%v, parsed=%v\n", ts, result)
		}
		return result, err
	}

	// Try "date" field as fallback
	if date, ok := txData["date"]; ok {
		result, err := parseTimestamp(date)
		if err == nil {
			fmt.Printf("[DEBUG-NORMALIZER] Extracted date: raw=%v, parsed=%v\n", date, result)
		}
		return result, err
	}

	return time.Time{}, fmt.Errorf("missing timestamp or date field")
}

// parseTimestamp parses a timestamp value into a time.Time.
func parseTimestamp(ts interface{}) (time.Time, error) {
	switch v := ts.(type) {
	case string:
		// Try RFC3339 format first
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t, nil
		}
		// Try RFC3339Nano format
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t, nil
		}
		return time.Time{}, fmt.Errorf("invalid timestamp format: %s", v)
	case time.Time:
		return v, nil
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp type: %T", ts)
	}
}

// extractMetadata extracts metadata from the transaction data.
// It specifically looks for payment_id and order_id in the metadata.
func extractMetadata(txData map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	metadataRaw, ok := txData["metadata"]
	if !ok {
		return result
	}

	metadata, ok := metadataRaw.(map[string]interface{})
	if !ok {
		return result
	}

	// Copy relevant metadata fields
	relevantFields := []string{"payment_id", "order_id", "reference", "description"}
	for _, field := range relevantFields {
		if val, exists := metadata[field]; exists {
			result[field] = val
		}
	}

	// Also include any custom metadata prefixed with "custom_" or without underscore prefix
	for key, val := range metadata {
		if _, exists := result[key]; !exists {
			result[key] = val
		}
	}

	return result
}
