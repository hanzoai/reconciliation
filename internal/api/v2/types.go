package v2

import (
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
)

// PolicyType represents the type of reconciliation policy.
type PolicyType string

const (
	PolicyTypeBalance     PolicyType = "balance"
	PolicyTypeTransaction PolicyType = "transaction"
)

// LedgerConfig represents the ledger configuration in a v2 policy request.
type LedgerConfig struct {
	Name  string                 `json:"name"`
	Query map[string]interface{} `json:"query,omitempty"`
}

// PaymentConfigBalance represents the payment configuration for balance mode.
type PaymentConfigBalance struct {
	PoolID string `json:"poolId"`
}

// PaymentConfigTransaction represents the payment configuration for transaction mode.
// ConnectorType and ConnectorID are mutually exclusive.
type PaymentConfigTransaction struct {
	ConnectorType *string `json:"connectorType,omitempty"`
	ConnectorID   *string `json:"connectorId,omitempty"`
}

// MatcherConfig represents the matcher configuration for transaction mode.
type MatcherConfig struct {
	DeterministicFields []string              `json:"deterministicFields,omitempty"`
	Scoring             *models.ScoringConfig `json:"scoring,omitempty"`
}

// CreatePolicyV2Request represents a v2 API request to create a policy.
type CreatePolicyV2Request struct {
	Name    string         `json:"name"`
	Type    PolicyType     `json:"type"`
	Ledger  LedgerConfig   `json:"ledger"`
	Payment interface{}    `json:"payment"`           // PaymentConfigBalance or PaymentConfigTransaction based on Type
	Matcher *MatcherConfig `json:"matcher,omitempty"` // Only for transaction type
}

// CreateBalancePolicyV2Request represents a v2 API request to create a balance policy.
type CreateBalancePolicyV2Request struct {
	Name    string               `json:"name"`
	Type    PolicyType           `json:"type"`
	Ledger  LedgerConfig         `json:"ledger"`
	Payment PaymentConfigBalance `json:"payment"`
}

// CreateTransactionPolicyV2Request represents a v2 API request to create a transaction policy.
type CreateTransactionPolicyV2Request struct {
	Name    string                   `json:"name"`
	Type    PolicyType               `json:"type"`
	Ledger  LedgerConfig             `json:"ledger"`
	Payment PaymentConfigTransaction `json:"payment"`
	Matcher *MatcherConfig           `json:"matcher,omitempty"`
}

// UpdatePolicyV2Request represents a v2 API request to update a policy.
type UpdatePolicyV2Request struct {
	Name    *string        `json:"name,omitempty"`
	Ledger  *LedgerConfig  `json:"ledger,omitempty"`
	Payment interface{}    `json:"payment,omitempty"` // Only for transaction type
	Matcher *MatcherConfig `json:"matcher,omitempty"` // Only for transaction type
}

// PolicyV2Response represents a v2 API response for a policy.
type PolicyV2Response struct {
	ID        uuid.UUID      `json:"id"`
	Name      string         `json:"name"`
	Type      PolicyType     `json:"type"`
	CreatedAt time.Time      `json:"createdAt"`
	Ledger    LedgerConfig   `json:"ledger"`
	Payment   interface{}    `json:"payment"`           // PaymentConfigBalance or PaymentConfigTransaction based on Type
	Matcher   *MatcherConfig `json:"matcher,omitempty"` // Only for transaction type
}

// BalancePolicyV2Response represents a v2 API response for a balance policy.
type BalancePolicyV2Response struct {
	ID        uuid.UUID            `json:"id"`
	Name      string               `json:"name"`
	Type      PolicyType           `json:"type"`
	CreatedAt time.Time            `json:"createdAt"`
	Ledger    LedgerConfig         `json:"ledger"`
	Payment   PaymentConfigBalance `json:"payment"`
}

// TransactionPolicyV2Response represents a v2 API response for a transaction policy.
type TransactionPolicyV2Response struct {
	ID        uuid.UUID                `json:"id"`
	Name      string                   `json:"name"`
	Type      PolicyType               `json:"type"`
	CreatedAt time.Time                `json:"createdAt"`
	Ledger    LedgerConfig             `json:"ledger"`
	Payment   PaymentConfigTransaction `json:"payment"`
	Matcher   *MatcherConfig           `json:"matcher,omitempty"`
}

// MatchRequest represents a v2 API request to run matching on transactions.
type MatchRequest struct {
	TransactionIDs []string `json:"transactionIds"`
}

// MatchResponse represents a v2 API response for match operation.
// Uses service.MatchResultV2 for the results.

// PolicyToV2Response converts a models.Policy to the appropriate v2 response.
func PolicyToV2Response(p *models.Policy) interface{} {
	ledger := LedgerConfig{
		Name:  p.LedgerName,
		Query: p.LedgerQuery,
	}

	if p.Mode == "transactional" {
		resp := TransactionPolicyV2Response{
			ID:        p.ID,
			Name:      p.Name,
			Type:      PolicyTypeTransaction,
			CreatedAt: p.CreatedAt,
			Ledger:    ledger,
			Payment: PaymentConfigTransaction{
				ConnectorType: p.ConnectorType,
				ConnectorID:   p.ConnectorID,
			},
		}

		// Add matcher config if present
		if len(p.DeterministicFields) > 0 || p.ScoringConfig != nil {
			resp.Matcher = &MatcherConfig{
				DeterministicFields: p.DeterministicFields,
				Scoring:             p.ScoringConfig,
			}
		}

		return resp
	}

	// Balance mode
	return BalancePolicyV2Response{
		ID:        p.ID,
		Name:      p.Name,
		Type:      PolicyTypeBalance,
		CreatedAt: p.CreatedAt,
		Ledger:    ledger,
		Payment: PaymentConfigBalance{
			PoolID: p.PaymentsPoolID.String(),
		},
	}
}
