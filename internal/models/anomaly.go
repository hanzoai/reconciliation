package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type AnomalyType string

const (
	AnomalyTypeMissingOnPayments AnomalyType = "MISSING_ON_PAYMENTS"
	AnomalyTypeMissingOnLedger   AnomalyType = "MISSING_ON_LEDGER"
	AnomalyTypeDuplicateLedger   AnomalyType = "DUPLICATE_LEDGER"
	AnomalyTypeAmountMismatch    AnomalyType = "AMOUNT_MISMATCH"
	AnomalyTypeCurrencyMismatch  AnomalyType = "CURRENCY_MISMATCH"
)

func (a AnomalyType) String() string {
	return string(a)
}

func (a AnomalyType) IsValid() bool {
	switch a {
	case AnomalyTypeMissingOnPayments, AnomalyTypeMissingOnLedger, AnomalyTypeDuplicateLedger, AnomalyTypeAmountMismatch, AnomalyTypeCurrencyMismatch:
		return true
	default:
		return false
	}
}

type Severity string

const (
	SeverityCritical Severity = "CRITICAL"
	SeverityHigh     Severity = "HIGH"
	SeverityMedium   Severity = "MEDIUM"
	SeverityLow      Severity = "LOW"
)

func (s Severity) String() string {
	return string(s)
}

func (s Severity) IsValid() bool {
	switch s {
	case SeverityCritical, SeverityHigh, SeverityMedium, SeverityLow:
		return true
	default:
		return false
	}
}

type AnomalyState string

const (
	AnomalyStateOpen     AnomalyState = "open"
	AnomalyStateResolved AnomalyState = "resolved"
)

func (s AnomalyState) String() string {
	return string(s)
}

func (s AnomalyState) IsValid() bool {
	switch s {
	case AnomalyStateOpen, AnomalyStateResolved:
		return true
	default:
		return false
	}
}

type Anomaly struct {
	bun.BaseModel `bun:"reconciliations.anomaly" json:"-"`

	ID            uuid.UUID    `bun:",pk,nullzero" json:"id"`
	PolicyID      *uuid.UUID   `bun:",nullzero" json:"policyID"`
	TransactionID *uuid.UUID   `bun:",nullzero" json:"transactionID"`
	Type          AnomalyType  `bun:",notnull" json:"type"`
	Severity      Severity     `bun:",notnull" json:"severity"`
	State         AnomalyState `bun:",notnull,default:'open'" json:"state"`
	Reason        string       `bun:"reason" json:"reason,omitempty"`
	CreatedAt     time.Time    `bun:",notnull" json:"createdAt"`
	ResolvedAt    *time.Time   `bun:"resolved_at" json:"resolvedAt,omitempty"`
	ResolvedBy    *string      `bun:"resolved_by" json:"resolvedBy,omitempty"`
}
