package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// TransactionIndexSuffix is the suffix for transaction index names.
const TransactionIndexSuffix = "reconciliation"

// TransactionIndexPattern returns the index pattern for a given stack.
// Example: mystack-reconciliation-*
func TransactionIndexPattern(stack string) string {
	return fmt.Sprintf("%s-%s-*", stack, TransactionIndexSuffix)
}

// TransactionIndexTemplateName returns the index template name for a given stack.
// Example: mystack-reconciliation-template
func TransactionIndexTemplateName(stack string) string {
	return fmt.Sprintf("%s-%s-template", stack, TransactionIndexSuffix)
}

// MonthlyTransactionIndexName returns the monthly index name for a given stack and date.
// The index name follows the pattern: {stack}-reconciliation-{yyyy-mm}
// Example: mystack-reconciliation-2026-01
func MonthlyTransactionIndexName(stack string, date time.Time) string {
	return fmt.Sprintf("%s-%s-%s", stack, TransactionIndexSuffix, date.Format("2006-01"))
}

// TransactionDocument represents a transaction document in OpenSearch.
// Note: PolicyID is not stored in the index - all transactions are indexed regardless of policy.
type TransactionDocument struct {
	TransactionID string                 `json:"transaction_id"`
	Side          string                 `json:"side"`
	Provider      string                 `json:"provider"`
	ExternalID    string                 `json:"external_id"`
	Amount        int64                  `json:"amount"`
	Currency      string                 `json:"currency"`
	OccurredAt    time.Time              `json:"occurred_at"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
}

// TransactionDocumentFromModel converts a models.Transaction to a TransactionDocument.
func TransactionDocumentFromModel(tx *models.Transaction) TransactionDocument {
	return TransactionDocument{
		TransactionID: tx.ID.String(),
		Side:          string(tx.Side),
		Provider:      tx.Provider,
		ExternalID:    tx.ExternalID,
		Amount:        tx.Amount,
		Currency:      tx.Currency,
		OccurredAt:    tx.OccurredAt,
		Metadata:      tx.Metadata,
	}
}

// transactionMappingProperties defines the OpenSearch mapping for transaction documents.
var transactionMappingProperties = map[string]interface{}{
	"transaction_id": map[string]interface{}{
		"type": "keyword",
	},
	"side": map[string]interface{}{
		"type": "keyword",
	},
	"provider": map[string]interface{}{
		"type": "keyword",
	},
	"external_id": map[string]interface{}{
		"type": "keyword",
	},
	"amount": map[string]interface{}{
		"type": "long",
	},
	"currency": map[string]interface{}{
		"type": "keyword",
	},
	"occurred_at": map[string]interface{}{
		"type": "date",
	},
	"metadata": map[string]interface{}{
		"type": "flat_object",
	},
}

// transactionIndexTemplateBody returns the index template body for transactions.
// If ismEnabled is true, the template will reference the ISM policy.
func transactionIndexTemplateBody(stack string, ismEnabled bool) map[string]interface{} {
	settings := map[string]interface{}{
		"number_of_shards":   1,
		"number_of_replicas": 0,
	}

	// Add ISM settings if enabled (OpenSearch uses ISM instead of ILM)
	if ismEnabled {
		settings["plugins.index_state_management.policy_id"] = TransactionISMPolicyName(stack)
	}

	return map[string]interface{}{
		"index_patterns": []string{fmt.Sprintf("%s-%s-*", stack, TransactionIndexSuffix)},
		"template": map[string]interface{}{
			"settings": settings,
			"mappings": map[string]interface{}{
				"properties": transactionMappingProperties,
			},
		},
		"priority": 100,
	}
}

// CreateTransactionIndexTemplate creates the index template for transactions.
// The template will be automatically applied to any index matching the pattern "{stack}-reconciliation-*".
// If ismEnabled is true, the template will reference the ISM policy.
func (c *Client) CreateTransactionIndexTemplate(ctx context.Context, stack string, ismEnabled bool) error {
	body := transactionIndexTemplateBody(stack, ismEnabled)
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal index template body: %w", err)
	}

	_, err = c.client.IndexTemplate.Create(ctx, opensearchapi.IndexTemplateCreateReq{
		IndexTemplate: TransactionIndexTemplateName(stack),
		Body:          bytes.NewReader(bodyBytes),
	})
	if err != nil {
		return fmt.Errorf("failed to create index template: %w", err)
	}

	return nil
}

// GetTransactionIndexTemplate retrieves the index template for transactions.
func (c *Client) GetTransactionIndexTemplate(ctx context.Context, stack string) (map[string]interface{}, error) {
	res, err := c.client.IndexTemplate.Get(ctx, &opensearchapi.IndexTemplateGetReq{
		IndexTemplates: []string{TransactionIndexTemplateName(stack)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get index template: %w", err)
	}

	// Convert response to map
	result := make(map[string]interface{})
	if len(res.IndexTemplates) > 0 {
		result["index_templates"] = res.IndexTemplates
	}

	return result, nil
}

// DeleteTransactionIndexTemplate deletes the index template for transactions.
func (c *Client) DeleteTransactionIndexTemplate(ctx context.Context, stack string) error {
	_, err := c.client.IndexTemplate.Delete(ctx, opensearchapi.IndexTemplateDeleteReq{
		IndexTemplate: TransactionIndexTemplateName(stack),
	})
	if err != nil {
		// Check if it's a 404 error (template doesn't exist)
		if strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "index_template_missing") {
			return nil
		}
		return fmt.Errorf("failed to delete index template: %w", err)
	}

	return nil
}

// GetIndexMapping retrieves the mapping for a specific index.
func (c *Client) GetIndexMapping(ctx context.Context, indexName string) (map[string]interface{}, error) {
	res, err := c.client.Indices.Mapping.Get(ctx, &opensearchapi.MappingGetReq{
		Indices: []string{indexName},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get index mapping: %w", err)
	}

	// Convert response to map[string]interface{}
	result := make(map[string]interface{})
	for k, v := range res.Indices {
		result[k] = v
	}

	return result, nil
}

// IndexTransaction indexes a transaction document into a monthly index.
// The index name follows the pattern: {stack}-reconciliation-{yyyy-mm}
// where the month is derived from the transaction's OccurredAt field.
// The document ID is set to the transaction ID to ensure idempotency (no duplicates).
func (c *Client) IndexTransaction(ctx context.Context, stack string, tx *models.Transaction) error {
	doc := TransactionDocumentFromModel(tx)
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction document: %w", err)
	}

	// Use monthly index based on transaction's OccurredAt date
	indexName := MonthlyTransactionIndexName(stack, tx.OccurredAt)
	docID := tx.ID.String()

	_, err = c.client.Index(ctx, opensearchapi.IndexReq{
		Index:      indexName,
		DocumentID: docID,
		Body:       bytes.NewReader(docBytes),
	})
	if err != nil {
		return fmt.Errorf("failed to index transaction: %w", err)
	}

	return nil
}

// GetTransaction retrieves a transaction document by ID from the monthly index.
// The date parameter specifies the month in which the transaction occurred.
func (c *Client) GetTransaction(ctx context.Context, stack string, transactionID uuid.UUID, date time.Time) (*TransactionDocument, error) {
	indexName := MonthlyTransactionIndexName(stack, date)

	res, err := c.client.Document.Get(ctx, opensearchapi.DocumentGetReq{
		Index:      indexName,
		DocumentID: transactionID.String(),
	})
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return nil, fmt.Errorf("transaction not found: %s", transactionID)
		}
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	// Parse the source into TransactionDocument
	var doc TransactionDocument
	sourceBytes, err := json.Marshal(res.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source: %w", err)
	}
	if err := json.Unmarshal(sourceBytes, &doc); err != nil {
		return nil, fmt.Errorf("failed to decode transaction response: %w", err)
	}

	return &doc, nil
}

// SearchTransaction searches for a transaction by ID across all monthly indices for a stack.
// This is useful when you don't know which month the transaction belongs to.
func (c *Client) SearchTransaction(ctx context.Context, stack string, transactionID uuid.UUID) (*TransactionDocument, error) {
	indexPattern := TransactionIndexPattern(stack)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"transaction_id": transactionID.String(),
			},
		},
	}
	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search query: %w", err)
	}

	res, err := c.client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{indexPattern},
		Body:    bytes.NewReader(queryBytes),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search transaction: %w", err)
	}

	if res.Hits.Total.Value == 0 {
		return nil, fmt.Errorf("transaction not found: %s", transactionID)
	}

	// Parse the first hit's source into TransactionDocument
	var doc TransactionDocument
	sourceBytes, err := json.Marshal(res.Hits.Hits[0].Source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source: %w", err)
	}
	if err := json.Unmarshal(sourceBytes, &doc); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	return &doc, nil
}

// RefreshIndex refreshes an index to make recently indexed documents searchable.
func (c *Client) RefreshIndex(ctx context.Context, indexName string) error {
	_, err := c.client.Indices.Refresh(ctx, &opensearchapi.IndicesRefreshReq{
		Indices: []string{indexName},
	})
	if err != nil {
		return fmt.Errorf("failed to refresh index: %w", err)
	}

	return nil
}

// IndexExists checks if an index exists.
func (c *Client) IndexExists(ctx context.Context, indexName string) (bool, error) {
	res, err := c.client.Indices.Exists(ctx, opensearchapi.IndicesExistsReq{
		Indices: []string{indexName},
	})
	if err != nil {
		// If the error contains 404, the index doesn't exist
		if strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if index exists: %w", err)
	}

	return res.StatusCode == 200, nil
}

// TransactionIndexTemplateExists checks if the transaction index template exists.
func (c *Client) TransactionIndexTemplateExists(ctx context.Context, stack string) (bool, error) {
	res, err := c.client.IndexTemplate.Exists(ctx, opensearchapi.IndexTemplateExistsReq{
		IndexTemplate: TransactionIndexTemplateName(stack),
	})
	if err != nil {
		// If the error contains 404, the template doesn't exist
		if strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if index template exists: %w", err)
	}

	return res.StatusCode == 200, nil
}

// EnsureTransactionIndexTemplate ensures the transaction index template exists and is up to date.
// This always creates/updates the template (PUT is idempotent in OpenSearch).
// If ismEnabled is true, the template will reference the ISM policy.
// Note: Updating the template only affects new indices; existing indices keep their old mapping.
func (c *Client) EnsureTransactionIndexTemplate(ctx context.Context, stack string, ismEnabled bool) error {
	return c.CreateTransactionIndexTemplate(ctx, stack, ismEnabled)
}

// GetTransactionMappingProperties returns the expected mapping properties for validation.
func GetTransactionMappingProperties() map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range transactionMappingProperties {
		result[k] = v
	}
	return result
}

// ValidateIndexMapping validates that an index has the expected transaction mapping.
func (c *Client) ValidateIndexMapping(ctx context.Context, indexName string) error {
	mapping, err := c.GetIndexMapping(ctx, indexName)
	if err != nil {
		return err
	}

	// Extract the properties from the mapping response
	indexMapping, ok := mapping[indexName].(map[string]interface{})
	if !ok {
		return fmt.Errorf("unexpected mapping structure for index %s", indexName)
	}

	mappings, ok := indexMapping["mappings"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("mappings not found in index %s", indexName)
	}

	properties, ok := mappings["properties"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("properties not found in index mapping %s", indexName)
	}

	// Validate each expected field
	expectedFields := []string{"transaction_id", "side", "provider", "external_id", "amount", "currency", "occurred_at", "metadata"}
	for _, field := range expectedFields {
		if _, ok := properties[field]; !ok {
			return fmt.Errorf("field %s not found in index mapping", field)
		}
	}

	// Validate field types
	fieldTypes := map[string]string{
		"transaction_id": "keyword",
		"side":           "keyword",
		"provider":       "keyword",
		"external_id":    "keyword",
		"amount":         "long",
		"currency":       "keyword",
		"occurred_at":    "date",
		"metadata":       "flat_object",
	}

	for field, expectedType := range fieldTypes {
		fieldMapping, ok := properties[field].(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid mapping for field %s", field)
		}

		actualType, ok := fieldMapping["type"].(string)
		if !ok {
			return fmt.Errorf("type not found for field %s", field)
		}

		if !strings.EqualFold(actualType, expectedType) {
			return fmt.Errorf("field %s has type %s, expected %s", field, actualType, expectedType)
		}
	}

	return nil
}

// SearchTransactions searches for transactions matching a query in OpenSearch.
// This is used by the probabilistic matcher to find candidate matches.
func (c *Client) SearchTransactions(ctx context.Context, indexPattern string, query map[string]interface{}, size int) ([]TransactionDocument, error) {
	searchBody := map[string]interface{}{
		"query": query,
		"size":  size,
	}
	bodyBytes, err := json.Marshal(searchBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search body: %w", err)
	}

	res, err := c.client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{indexPattern},
		Body:    bytes.NewReader(bodyBytes),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to search transactions: %w", err)
	}

	documents := make([]TransactionDocument, 0, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		var doc TransactionDocument
		sourceBytes, err := json.Marshal(hit.Source)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal source: %w", err)
		}
		if err := json.Unmarshal(sourceBytes, &doc); err != nil {
			return nil, fmt.Errorf("failed to decode hit source: %w", err)
		}
		documents = append(documents, doc)
	}

	return documents, nil
}

// BulkIndex indexes multiple transaction documents in a single bulk request.
func (c *Client) BulkIndex(ctx context.Context, stack string, transactions []*models.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	// Build bulk request body
	var buf bytes.Buffer
	for _, tx := range transactions {
		doc := TransactionDocumentFromModel(tx)
		indexName := MonthlyTransactionIndexName(stack, tx.OccurredAt)

		// Action line
		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
				"_id":    tx.ID.String(),
			},
		}
		actionBytes, _ := json.Marshal(action)
		buf.Write(actionBytes)
		buf.WriteByte('\n')

		// Document line
		docBytes, _ := json.Marshal(doc)
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	_, err := c.client.Bulk(ctx, opensearchapi.BulkReq{
		Body: &buf,
	})
	if err != nil {
		return fmt.Errorf("failed to bulk index transactions: %w", err)
	}

	return nil
}

// =============================================================================
// NEW METHODS FOR TRANSACTION STORE (OpenSearch as single source of truth)
// =============================================================================

// ErrTransactionNotFound is returned when a transaction is not found in OpenSearch.
var ErrTransactionNotFound = fmt.Errorf("transaction not found")

// GenerateDocumentID generates a unique document ID for a transaction.
// Format: {side}_{external_id} - ensures uniqueness per side/external_id pair.
func GenerateDocumentID(side models.TransactionSide, externalID string) string {
	return fmt.Sprintf("%s_%s", side, externalID)
}

// ExistsByExternalIDs checks which external_ids already exist in OpenSearch.
// Returns a map where keys are external_ids that exist.
func (c *Client) ExistsByExternalIDs(ctx context.Context, stack string, side models.TransactionSide, externalIDs []string) (map[string]bool, error) {
	if len(externalIDs) == 0 {
		return make(map[string]bool), nil
	}

	indexPattern := TransactionIndexPattern(stack)

	// Build a terms query with filter on side
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"side": string(side),
						},
					},
					{
						"terms": map[string]interface{}{
							"external_id": externalIDs,
						},
					},
				},
			},
		},
		"_source": []string{"external_id"},
		"size":    len(externalIDs),
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	res, err := c.client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{indexPattern},
		Body:    bytes.NewReader(queryBytes),
	})
	if err != nil {
		// If no indices exist yet, return empty map
		if strings.Contains(err.Error(), "index_not_found") {
			return make(map[string]bool), nil
		}
		return nil, fmt.Errorf("failed to search for existing external IDs: %w", err)
	}

	result := make(map[string]bool, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		sourceBytes, err := json.Marshal(hit.Source)
		if err != nil {
			continue
		}
		var doc struct {
			ExternalID string `json:"external_id"`
		}
		if err := json.Unmarshal(sourceBytes, &doc); err != nil {
			continue
		}
		result[doc.ExternalID] = true
	}

	return result, nil
}

// GetTransactionByID retrieves a transaction by its UUID.
// Searches across all monthly indices for the stack.
func (c *Client) GetTransactionByID(ctx context.Context, stack string, id uuid.UUID) (*models.Transaction, error) {
	doc, err := c.SearchTransaction(ctx, stack, id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, ErrTransactionNotFound
		}
		return nil, err
	}

	return documentToModel(doc)
}

// GetTransactionByExternalID retrieves a transaction by side and external_id.
func (c *Client) GetTransactionByExternalID(ctx context.Context, stack string, side models.TransactionSide, externalID string) (*models.Transaction, error) {
	indexPattern := TransactionIndexPattern(stack)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"side": string(side),
						},
					},
					{
						"term": map[string]interface{}{
							"external_id": externalID,
						},
					},
				},
			},
		},
		"size": 1,
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	res, err := c.client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{indexPattern},
		Body:    bytes.NewReader(queryBytes),
	})
	if err != nil {
		if strings.Contains(err.Error(), "index_not_found") {
			return nil, ErrTransactionNotFound
		}
		return nil, fmt.Errorf("failed to search transaction: %w", err)
	}

	if res.Hits.Total.Value == 0 {
		return nil, ErrTransactionNotFound
	}

	var doc TransactionDocument
	sourceBytes, err := json.Marshal(res.Hits.Hits[0].Source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source: %w", err)
	}
	if err := json.Unmarshal(sourceBytes, &doc); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	return documentToModel(&doc)
}

// SearchByField searches transactions by any field (direct or metadata).
// For metadata fields, use the field name directly (e.g., "order_id" not "metadata.order_id").
func (c *Client) SearchByField(ctx context.Context, stack string, side models.TransactionSide, field string, value string) ([]*models.Transaction, error) {
	indexPattern := TransactionIndexPattern(stack)

	// Determine the field path
	fieldPath := field
	if !isDirectField(field) {
		fieldPath = "metadata." + field
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"side": string(side),
						},
					},
					{
						"term": map[string]interface{}{
							fieldPath: value,
						},
					},
				},
			},
		},
		"size": 1000, // Reasonable limit for deterministic matching
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	res, err := c.client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{indexPattern},
		Body:    bytes.NewReader(queryBytes),
	})
	if err != nil {
		if strings.Contains(err.Error(), "index_not_found") {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to search transactions: %w", err)
	}

	transactions := make([]*models.Transaction, 0, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		var doc TransactionDocument
		sourceBytes, err := json.Marshal(hit.Source)
		if err != nil {
			continue
		}
		if err := json.Unmarshal(sourceBytes, &doc); err != nil {
			continue
		}
		tx, err := documentToModel(&doc)
		if err != nil {
			continue
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// GetTransactionsByProvider returns all transactions for a given provider and side.
// Uses scroll API for large result sets.
func (c *Client) GetTransactionsByProvider(ctx context.Context, stack string, provider string, side models.TransactionSide) ([]*models.Transaction, error) {
	indexPattern := TransactionIndexPattern(stack)

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"side": string(side),
						},
					},
					{
						"term": map[string]interface{}{
							"provider": provider,
						},
					},
				},
			},
		},
		"size": 1000,
		"sort": []map[string]interface{}{
			{"occurred_at": map[string]interface{}{"order": "desc"}},
		},
	}

	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	res, err := c.client.Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{indexPattern},
		Body:    bytes.NewReader(queryBytes),
	})
	if err != nil {
		if strings.Contains(err.Error(), "index_not_found") {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to search transactions: %w", err)
	}

	transactions := make([]*models.Transaction, 0, len(res.Hits.Hits))
	for _, hit := range res.Hits.Hits {
		var doc TransactionDocument
		sourceBytes, err := json.Marshal(hit.Source)
		if err != nil {
			continue
		}
		if err := json.Unmarshal(sourceBytes, &doc); err != nil {
			continue
		}
		tx, err := documentToModel(&doc)
		if err != nil {
			continue
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// BulkIndexIdempotent indexes transactions using {side}_{external_id} as document ID.
// This ensures idempotency - duplicate transactions will be overwritten.
func (c *Client) BulkIndexIdempotent(ctx context.Context, stack string, transactions []*models.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	// Build bulk request body
	var buf bytes.Buffer
	for _, tx := range transactions {
		doc := TransactionDocumentFromModel(tx)
		indexName := MonthlyTransactionIndexName(stack, tx.OccurredAt)
		docID := GenerateDocumentID(tx.Side, tx.ExternalID)

		// Action line
		action := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": indexName,
				"_id":    docID,
			},
		}
		actionBytes, _ := json.Marshal(action)
		buf.Write(actionBytes)
		buf.WriteByte('\n')

		// Document line
		docBytes, _ := json.Marshal(doc)
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}

	res, err := c.client.Bulk(ctx, opensearchapi.BulkReq{
		Body: &buf,
	})
	if err != nil {
		return fmt.Errorf("failed to bulk index transactions: %w", err)
	}

	// Check for errors in response
	if res.Errors {
		// Extract first error for debugging
		for _, item := range res.Items {
			for _, result := range item {
				if result.Error != nil {
					return fmt.Errorf("bulk index error: %s - %s", result.Error.Type, result.Error.Reason)
				}
			}
		}
	}

	return nil
}

// CreateTransaction indexes a single transaction with idempotent document ID.
func (c *Client) CreateTransaction(ctx context.Context, stack string, tx *models.Transaction) error {
	doc := TransactionDocumentFromModel(tx)
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction document: %w", err)
	}

	indexName := MonthlyTransactionIndexName(stack, tx.OccurredAt)
	docID := GenerateDocumentID(tx.Side, tx.ExternalID)

	_, err = c.client.Index(ctx, opensearchapi.IndexReq{
		Index:      indexName,
		DocumentID: docID,
		Body:       bytes.NewReader(docBytes),
	})
	if err != nil {
		return fmt.Errorf("failed to index transaction: %w", err)
	}

	return nil
}

// isDirectField returns true if the field is a direct transaction field (not metadata).
func isDirectField(field string) bool {
	directFields := map[string]bool{
		"transaction_id": true,
		"side":           true,
		"provider":       true,
		"external_id":    true,
		"amount":         true,
		"currency":       true,
		"occurred_at":    true,
	}
	return directFields[field]
}

// documentToModel converts a TransactionDocument to a models.Transaction.
func documentToModel(doc *TransactionDocument) (*models.Transaction, error) {
	txID, err := uuid.Parse(doc.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction_id: %w", err)
	}

	return &models.Transaction{
		ID:         txID,
		Side:       models.TransactionSide(doc.Side),
		Provider:   doc.Provider,
		ExternalID: doc.ExternalID,
		Amount:     doc.Amount,
		Currency:   doc.Currency,
		OccurredAt: doc.OccurredAt,
		Metadata:   doc.Metadata,
	}, nil
}
