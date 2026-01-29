package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
)

// APIClient is an HTTP client for the reconciliation API with OAuth2 authentication.
type APIClient struct {
	stackURL     string
	clientID     string
	clientSecret string
	httpClient   *http.Client

	// Token management
	accessToken string
	tokenExpiry time.Time
	tokenMu     sync.Mutex
}

// NewAPIClient creates a new API client.
// If clientID and clientSecret are empty, no authentication is used (for local testing).
func NewAPIClient(stackURL, clientID, clientSecret string) *APIClient {
	return &APIClient{
		stackURL:     strings.TrimSuffix(stackURL, "/"),
		clientID:     clientID,
		clientSecret: clientSecret,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
	}
}

// TokenResponse represents an OAuth2 token response.
type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

// getToken retrieves or refreshes the OAuth2 access token.
func (c *APIClient) getToken(ctx context.Context) (string, error) {
	// If no credentials, skip authentication
	if c.clientID == "" || c.clientSecret == "" {
		return "", nil
	}

	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	// Check if we have a valid token
	if c.accessToken != "" && time.Now().Before(c.tokenExpiry) {
		return c.accessToken, nil
	}

	// Request a new token
	tokenURL := fmt.Sprintf("%s/api/auth/oauth/token", c.stackURL)

	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("scope", "reconciliation:read reconciliation:write")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// Use Basic Auth for client credentials
	req.SetBasicAuth(c.clientID, c.clientSecret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to request token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("failed to decode token response: %w", err)
	}

	c.accessToken = tokenResp.AccessToken
	// Set expiry with a 30-second buffer
	c.tokenExpiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn-30) * time.Second)

	return c.accessToken, nil
}

// Do performs an HTTP request with optional authentication.
func (c *APIClient) Do(ctx context.Context, method, path string, body interface{}) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	// Determine the request URL
	var reqURL string
	if c.clientID != "" && c.clientSecret != "" {
		// Stack mode: use /api/reconciliation prefix
		reqURL = fmt.Sprintf("%s/api/reconciliation%s", c.stackURL, path)
	} else {
		// Local mode: direct API access
		reqURL = fmt.Sprintf("%s%s", c.stackURL, path)
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add authentication if credentials are configured
	token, err := c.getToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get token: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	return c.httpClient.Do(req)
}

// DoJSON performs a request and decodes the JSON response.
func (c *APIClient) DoJSON(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	resp, err := c.Do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to decode response: %w (body: %s)", err, string(respBody))
		}
	}

	return nil
}

// API Response Types

// APIResponse represents a generic API response wrapper.
type APIResponse struct {
	Data   json.RawMessage `json:"data"`
	Cursor *CursorResponse `json:"cursor,omitempty"`
}

// CursorResponse represents pagination cursor.
type CursorResponse struct {
	PageSize int             `json:"pageSize"`
	HasMore  bool            `json:"hasMore"`
	Next     string          `json:"next,omitempty"`
	Data     json.RawMessage `json:"data"`
}

// ScoringWeights defines weights for scoring criteria.
type ScoringWeights struct {
	Amount   float64 `json:"amount,omitempty"`
	Date     float64 `json:"date,omitempty"`
	Metadata float64 `json:"metadata,omitempty"`
}

// ScoringThresholds defines score thresholds for match decisions.
type ScoringThresholds struct {
	AutoMatch float64 `json:"autoMatch,omitempty"`
	Review    float64 `json:"review,omitempty"`
}

// ScoringConfig defines scoring parameters for probabilistic matching.
type ScoringConfig struct {
	TimeWindowHours         int                `json:"timeWindowHours,omitempty"`
	AmountTolerancePercent  float64            `json:"amountTolerancePercent,omitempty"`
	MetadataFields          []string           `json:"metadataFields,omitempty"`
	MetadataCaseInsensitive bool               `json:"metadataCaseInsensitive,omitempty"`
	Weights                 *ScoringWeights    `json:"weights,omitempty"`
	Thresholds              *ScoringThresholds `json:"thresholds,omitempty"`
}

// V2 API Types

// LedgerConfigV2 represents the ledger configuration in V2 API.
type LedgerConfigV2 struct {
	Name  string                 `json:"name"`
	Query map[string]interface{} `json:"query,omitempty"`
}

// PaymentConfigTransactionV2 represents the payment configuration for transaction mode in V2 API.
type PaymentConfigTransactionV2 struct {
	ConnectorType *string `json:"connectorType,omitempty"`
	ConnectorID   *string `json:"connectorId,omitempty"`
}

// MatcherConfigV2 represents the matcher configuration in V2 API.
type MatcherConfigV2 struct {
	DeterministicFields []string              `json:"deterministicFields,omitempty"`
	Scoring             *models.ScoringConfig `json:"scoring,omitempty"`
}

// CreatePolicyV2Request represents a V2 API request to create a transaction policy.
type CreatePolicyV2Request struct {
	Name    string                     `json:"name"`
	Type    string                     `json:"type"` // "transaction"
	Ledger  LedgerConfigV2             `json:"ledger"`
	Payment PaymentConfigTransactionV2 `json:"payment"`
	Matcher *MatcherConfigV2           `json:"matcher,omitempty"`
}

// PolicyV2Response represents a policy returned by the V2 API.
type PolicyV2Response struct {
	ID        string                     `json:"id"`
	Name      string                     `json:"name"`
	Type      string                     `json:"type"`
	CreatedAt string                     `json:"createdAt"`
	Ledger    LedgerConfigV2             `json:"ledger"`
	Payment   PaymentConfigTransactionV2 `json:"payment"`
	Matcher   *MatcherConfigV2           `json:"matcher,omitempty"`
}

// PolicyResponse represents a policy returned by the V1 API (for listing).
type PolicyResponse struct {
	ID                  string                 `json:"id"`
	Name                string                 `json:"name"`
	CreatedAt           string                 `json:"createdAt"`
	LedgerName          string                 `json:"ledgerName"`
	LedgerQuery         map[string]interface{} `json:"ledgerQuery"`
	PaymentsPoolID      string                 `json:"paymentsPoolID"`
	Mode                string                 `json:"mode"`
	Topology            string                 `json:"topology"`
	DeterministicFields []string               `json:"deterministicFields,omitempty"`
	ScoringConfig       *ScoringConfig         `json:"scoringConfig,omitempty"`
}

// MatchResponse represents a match returned by the API.
type MatchResponse struct {
	ID                     string    `json:"id"`
	PolicyID               string    `json:"policyId"`
	LedgerTransactionIDs   []string  `json:"ledgerTransactionIds"`
	PaymentsTransactionIDs []string  `json:"paymentsTransactionIds"`
	Score                  float64   `json:"score"`
	Decision               string    `json:"decision"`
	CreatedAt              time.Time `json:"createdAt"`
}

// AnomalyResponse represents an anomaly returned by the API.
type AnomalyResponse struct {
	ID            string     `json:"id"`
	PolicyID      string     `json:"policyID"`
	TransactionID string     `json:"transactionID"`
	Type          string     `json:"type"`
	Severity      string     `json:"severity"`
	State         string     `json:"state"`
	Reason        string     `json:"reason,omitempty"`
	CreatedAt     time.Time  `json:"createdAt"`
	ResolvedAt    *time.Time `json:"resolvedAt,omitempty"`
	ResolvedBy    string     `json:"resolvedBy,omitempty"`
}

// TransactionResponse represents a transaction returned by the API.
type TransactionResponse struct {
	ID         string                 `json:"id"`
	ExternalID string                 `json:"externalId"`
	Side       string                 `json:"side"`
	Provider   string                 `json:"provider"`
	Amount     int64                  `json:"amount"`
	Currency   string                 `json:"currency"`
	OccurredAt time.Time              `json:"occurredAt"`
	IngestedAt time.Time              `json:"ingestedAt"`
	Metadata   map[string]interface{} `json:"metadata"`
}

// Client Methods

// ListPoliciesV2 retrieves all policies from the V2 API.
func (c *APIClient) ListPoliciesV2(ctx context.Context) ([]PolicyV2Response, error) {
	var response APIResponse
	err := c.DoJSON(ctx, http.MethodGet, "/v2/policies", nil, &response)
	if err != nil {
		return nil, err
	}

	// Try to parse from cursor first
	if response.Cursor != nil && len(response.Cursor.Data) > 0 {
		var policies []PolicyV2Response
		if err := json.Unmarshal(response.Cursor.Data, &policies); err != nil {
			return nil, fmt.Errorf("failed to parse policies from cursor: %w", err)
		}
		return policies, nil
	}

	// Try to parse from data
	if len(response.Data) > 0 {
		var policies []PolicyV2Response
		if err := json.Unmarshal(response.Data, &policies); err != nil {
			return nil, fmt.Errorf("failed to parse policies: %w", err)
		}
		return policies, nil
	}

	return nil, nil
}

// CreatePolicyV2 creates a policy using the V2 API.
func (c *APIClient) CreatePolicyV2(ctx context.Context, req *CreatePolicyV2Request) (*PolicyV2Response, error) {
	var response APIResponse
	err := c.DoJSON(ctx, http.MethodPost, "/v2/policies", req, &response)
	if err != nil {
		return nil, err
	}

	var policy PolicyV2Response
	if err := json.Unmarshal(response.Data, &policy); err != nil {
		return nil, fmt.Errorf("failed to parse policy response: %w", err)
	}

	return &policy, nil
}

// DeletePolicy deletes a policy by ID.
func (c *APIClient) DeletePolicy(ctx context.Context, policyID string) error {
	resp, err := c.Do(ctx, http.MethodDelete, "/v2/policies/"+policyID, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete policy: status %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetPolicyAnomalies retrieves all anomalies for a specific policy, following pagination.
func (c *APIClient) GetPolicyAnomalies(ctx context.Context, policyID string) ([]AnomalyResponse, error) {
	return c.fetchAllAnomalies(ctx, "/v2/policies/"+policyID+"/anomalies")
}

// fetchAllAnomalies fetches all pages of anomalies from a paginated endpoint.
func (c *APIClient) fetchAllAnomalies(ctx context.Context, basePath string) ([]AnomalyResponse, error) {
	var all []AnomalyResponse
	path := basePath

	for {
		var response APIResponse
		if err := c.DoJSON(ctx, http.MethodGet, path, nil, &response); err != nil {
			return nil, err
		}

		var page []AnomalyResponse
		if response.Cursor != nil && len(response.Cursor.Data) > 0 {
			if err := json.Unmarshal(response.Cursor.Data, &page); err != nil {
				return nil, fmt.Errorf("failed to parse anomalies: %w", err)
			}
			all = append(all, page...)

			if !response.Cursor.HasMore || response.Cursor.Next == "" {
				break
			}
			path = basePath + "?cursor=" + response.Cursor.Next
			continue
		}

		if len(response.Data) > 0 {
			if err := json.Unmarshal(response.Data, &page); err != nil {
				return nil, fmt.Errorf("failed to parse anomalies: %w", err)
			}
			all = append(all, page...)
		}
		break
	}

	return all, nil
}

// CleanupTransactionsRequest represents a request to cleanup transactions.
type CleanupTransactionsRequest struct {
	Provider string `json:"provider"`
	Side     string `json:"side"`
}

// CleanupTransactions deletes all transactions for a given provider and side.
func (c *APIClient) CleanupTransactions(ctx context.Context, provider, side string) (int64, error) {
	req := CleanupTransactionsRequest{
		Provider: provider,
		Side:     side,
	}

	var response struct {
		Deleted int64 `json:"deleted"`
	}
	err := c.DoJSON(ctx, http.MethodDelete, "/v2/admin/transactions", req, &response)
	if err != nil {
		return 0, err
	}

	return response.Deleted, nil
}

// GetAllAnomalies retrieves all anomalies across all policies, following pagination.
func (c *APIClient) GetAllAnomalies(ctx context.Context) ([]AnomalyResponse, error) {
	return c.fetchAllAnomalies(ctx, "/v2/anomalies")
}

