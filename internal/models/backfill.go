package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

// BackfillStatus represents the status of a backfill operation.
type BackfillStatus string

const (
	BackfillStatusPending   BackfillStatus = "pending"
	BackfillStatusRunning   BackfillStatus = "running"
	BackfillStatusCompleted BackfillStatus = "completed"
	BackfillStatusFailed    BackfillStatus = "failed"
)

func (s BackfillStatus) String() string {
	return string(s)
}

func (s BackfillStatus) IsValid() bool {
	switch s {
	case BackfillStatusPending, BackfillStatusRunning, BackfillStatusCompleted, BackfillStatusFailed:
		return true
	}
	return false
}

// BackfillSource represents the data source for a backfill.
type BackfillSource string

const (
	BackfillSourceLedger   BackfillSource = "ledger"
	BackfillSourcePayments BackfillSource = "payments"
)

func (s BackfillSource) String() string {
	return string(s)
}

func (s BackfillSource) IsValid() bool {
	switch s {
	case BackfillSourceLedger, BackfillSourcePayments:
		return true
	}
	return false
}

// Backfill represents a historical data backfill operation.
type Backfill struct {
	bun.BaseModel `bun:"reconciliations.backfill" json:"-"`

	ID           uuid.UUID      `bun:",pk,nullzero" json:"id"`
	Source       BackfillSource `bun:",notnull" json:"source"`
	Ledger       *string        `bun:",nullzero" json:"ledger,omitempty"`
	Since        time.Time      `bun:",notnull" json:"since"`
	Status       BackfillStatus `bun:",notnull" json:"status"`
	Ingested     int64          `bun:",notnull,default:0" json:"ingested"`
	LastCursor   *string        `bun:",nullzero" json:"lastCursor,omitempty"`
	ErrorMessage *string        `bun:",nullzero" json:"error,omitempty"`
	CreatedAt    time.Time      `bun:",notnull" json:"createdAt"`
	UpdatedAt    time.Time      `bun:",notnull" json:"updatedAt"`
}
