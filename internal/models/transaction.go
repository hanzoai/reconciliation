package models

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type TransactionSide string

const (
	TransactionSideLedger   TransactionSide = "LEDGER"
	TransactionSidePayments TransactionSide = "PAYMENTS"
)

func (t TransactionSide) String() string {
	return string(t)
}

type Transaction struct {
	bun.BaseModel `bun:"reconciliations.transaction" json:"-"`

	ID         uuid.UUID              `bun:",pk,nullzero" json:"id"`
	PolicyID   *uuid.UUID             `bun:",nullzero" json:"policyID,omitempty"`
	Side       TransactionSide        `bun:",notnull" json:"side"`
	Provider   string                 `bun:",notnull" json:"provider"`
	ExternalID string                 `bun:",notnull" json:"externalID"`
	Amount     int64                  `bun:",notnull" json:"amount"`
	Currency   string                 `bun:",notnull" json:"currency"`
	OccurredAt time.Time              `bun:",notnull" json:"occurredAt"`
	IngestedAt time.Time              `bun:",notnull" json:"ingestedAt"`
	Metadata   map[string]interface{} `bun:",type:jsonb" json:"metadata"`
}

func (t TransactionSide) IsValid() bool {
	switch t {
	case TransactionSideLedger, TransactionSidePayments:
		return true
	default:
		return false
	}
}

func (t *Transaction) Validate() error {
	// PolicyID is optional - transactions can be ingested without a policy
	if t.Side == "" {
		return errors.New("side is required")
	}
	if !t.Side.IsValid() {
		return errors.New("side must be LEDGER or PAYMENTS")
	}
	if t.Provider == "" {
		return errors.New("provider is required")
	}
	if t.ExternalID == "" {
		return errors.New("externalID is required")
	}
	if t.Currency == "" {
		return errors.New("currency is required")
	}
	if t.OccurredAt.IsZero() {
		return errors.New("occurredAt is required")
	}
	return nil
}
