package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type AssertionMode string

const (
	AssertionModeCoverage  AssertionMode = "COVERAGE"
	AssertionModeEquality  AssertionMode = "EQUALITY"
	AssertionModeMinBuffer AssertionMode = "MIN_BUFFER"
)

func (a AssertionMode) String() string {
	return string(a)
}

func (a AssertionMode) IsValid() bool {
	switch a {
	case AssertionModeCoverage, AssertionModeEquality, AssertionModeMinBuffer:
		return true
	default:
		return false
	}
}

func NormalizeAssertionMode(a AssertionMode) AssertionMode {
	if a == "" {
		return AssertionModeCoverage
	}
	return a
}

type Policy struct {
	bun.BaseModel `bun:"reconciliations.policy" json:"-"`

	// Policy Related fields
	ID        uuid.UUID `bun:",pk,nullzero" json:"id"`
	CreatedAt time.Time `bun:",notnull" json:"createdAt"`
	Name      string    `bun:",notnull" json:"name"`

	// Reconciliation Needed fields
	LedgerName      string                 `bun:",notnull" json:"ledgerName"`
	LedgerQuery     map[string]interface{} `bun:",type:jsonb,notnull" json:"ledgerQuery"`
	PaymentsPoolID  uuid.UUID              `bun:",notnull" json:"paymentsPoolID"`
	AssertionMode   AssertionMode          `bun:",notnull,default:'COVERAGE'" json:"assertionMode"`
	AssertionConfig map[string]interface{} `bun:",type:jsonb,notnull,default:'{}'" json:"assertionConfig"`
}
