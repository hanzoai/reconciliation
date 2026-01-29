package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// ScoringWeights defines weights for different scoring criteria in transactional matching.
type ScoringWeights struct {
	Amount   float64 `json:"amount,omitempty"`
	Date     float64 `json:"date,omitempty"`
	Metadata float64 `json:"metadata,omitempty"`
}

// ScoringThresholds defines score thresholds for match decisions.
type ScoringThresholds struct {
	AutoMatch float64 `json:"autoMatch,omitempty"` // Score >= this threshold -> MATCHED
	Review    float64 `json:"review,omitempty"`    // Score >= this threshold (but < AutoMatch) -> REQUIRES_REVIEW
}

// ScoringConfig defines scoring parameters for transactional reconciliation.
type ScoringConfig struct {
	TimeWindowHours          int                `json:"timeWindowHours,omitempty"`          // Max time difference for matching transactions
	AmountTolerancePercent   float64            `json:"amountTolerancePercent,omitempty"`   // Allowed percentage difference in amounts
	MetadataFields           []string           `json:"metadataFields,omitempty"`           // Fields to compare for metadata overlap scoring; defaults to ["order_id", "user_id"]
	MetadataCaseInsensitive  bool               `json:"metadataCaseInsensitive,omitempty"`  // If true, metadata comparison is case-insensitive
	Weights                  *ScoringWeights    `json:"weights,omitempty"`
	Thresholds               *ScoringThresholds `json:"thresholds,omitempty"`
}

type Policy struct {
	bun.BaseModel `bun:"reconciliations.policy" json:"-"`

	// Policy Related fields
	ID        uuid.UUID `bun:",pk,nullzero" json:"id"`
	CreatedAt time.Time `bun:",notnull" json:"createdAt"`
	Name      string    `bun:",notnull" json:"name"`
	Mode      string    `bun:"policy_mode,notnull,default:'balance'" json:"mode"` // Allowed values: 'balance', 'transactional'
	Topology  string    `bun:",notnull,default:'1:1'" json:"topology"` // Allowed values: '1:1', '1:N', 'N:1'

	// Reconciliation Needed fields
	LedgerName          string                 `bun:",notnull" json:"ledgerName"`
	LedgerQuery         map[string]interface{} `bun:",type:jsonb,notnull" json:"ledgerQuery"`
	PaymentsPoolID      uuid.UUID              `bun:",notnull" json:"paymentsPoolID"`
	PaymentsProvider    string                 `bun:",nullzero" json:"paymentsProvider,omitempty"` // Provider name for dynamic payments lookup (e.g., "stripe", "wise")
	DeterministicFields []string               `bun:",type:text[],array" json:"deterministicFields,omitempty"` // NULL interpreted as ['external_id']
	ScoringConfig       *ScoringConfig         `bun:",type:jsonb" json:"scoringConfig,omitempty"`              // NULL uses engine defaults

	// V2 API fields - for more specific connector targeting (mutually exclusive)
	ConnectorType *string `bun:",nullzero" json:"connectorType,omitempty"` // Connector type (e.g., "stripe", "wise") - v2 API
	ConnectorID   *string `bun:",nullzero" json:"connectorId,omitempty"`   // Specific connector ID - v2 API (takes precedence over ConnectorType)
}

// IsTransactional returns true if the policy is configured for transactional reconciliation mode.
// Returns false if Mode is empty (backward compatibility) or set to 'balance'.
func (p *Policy) IsTransactional() bool {
	return p.Mode == "transactional"
}
