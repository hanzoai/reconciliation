package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type Decision string

const (
	DecisionMatched        Decision = "MATCHED"
	DecisionRequiresReview Decision = "REQUIRES_REVIEW"
	DecisionUnmatched      Decision = "UNMATCHED"
	DecisionManual         Decision = "MANUAL"
	DecisionManualMatch    Decision = "MANUAL_MATCH"
)

func (d Decision) String() string {
	return string(d)
}

func (d Decision) IsValid() bool {
	switch d {
	case DecisionMatched, DecisionRequiresReview, DecisionUnmatched, DecisionManual, DecisionManualMatch:
		return true
	default:
		return false
	}
}

type Explanation struct {
	Reason     string                 `json:"reason"`
	Score      float64                `json:"score,omitempty"`
	FieldNotes map[string]string      `json:"fieldNotes,omitempty"`
	Details    map[string]interface{} `json:"details,omitempty"`
}

type Match struct {
	bun.BaseModel `bun:"reconciliations.match" json:"-"`

	ID                     uuid.UUID   `bun:"id,pk,nullzero" json:"id"`
	PolicyID               uuid.UUID   `bun:"policy_id,nullzero" json:"policyID"`
	LedgerTransactionIDs   []uuid.UUID `bun:"ledger_tx_ids,type:uuid[],array" json:"ledgerTransactionIDs"`
	PaymentsTransactionIDs []uuid.UUID `bun:"payment_tx_ids,type:uuid[],array" json:"paymentsTransactionIDs"`
	Score                  float64     `bun:"score,notnull" json:"score"`
	Decision               Decision    `bun:"decision,notnull" json:"decision"`
	Explanation            Explanation `bun:"explanation,type:jsonb" json:"explanation"`
	CreatedAt              time.Time   `bun:"created_at,notnull" json:"createdAt"`
}
