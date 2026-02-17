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

type PolicyLifecycle string

const (
	PolicyLifecycleEnabled  PolicyLifecycle = "ENABLED"
	PolicyLifecycleDisabled PolicyLifecycle = "DISABLED"
	PolicyLifecycleArchived PolicyLifecycle = "ARCHIVED"
)

func (l PolicyLifecycle) String() string {
	return string(l)
}

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

func NormalizePolicyLifecycle(l PolicyLifecycle) PolicyLifecycle {
	if l == "" {
		return PolicyLifecycleEnabled
	}
	return l
}

type Policy struct {
	bun.BaseModel `bun:"reconciliations.policy" json:"-"`

	// Policy Related fields
	ID        uuid.UUID       `bun:",pk,nullzero" json:"id"` // Version row id
	PolicyID  uuid.UUID       `bun:",notnull" json:"policyID"`
	Version   int64           `bun:",notnull" json:"version"`
	CreatedAt time.Time       `bun:",notnull" json:"createdAt"`
	Name      string          `bun:",notnull" json:"name"`
	Lifecycle PolicyLifecycle `bun:",notnull,default:'ENABLED'" json:"lifecycle"`

	// Reconciliation Needed fields
	LedgerName      string                 `bun:",notnull" json:"ledgerName"`
	LedgerQuery     map[string]interface{} `bun:",type:jsonb,notnull" json:"ledgerQuery"`
	PaymentsPoolID  uuid.UUID              `bun:",notnull" json:"paymentsPoolID"`
	AssertionMode   AssertionMode          `bun:",notnull,default:'COVERAGE'" json:"mode"`
	AssertionConfig map[string]interface{} `bun:",type:jsonb,notnull,default:'{}'" json:"assertionConfig"`
}
