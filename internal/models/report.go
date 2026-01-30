package models

import (
	"time"

	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type Report struct {
	bun.BaseModel `bun:"reconciliations.report" json:"-"`

	ID                uuid.UUID        `bun:",pk,nullzero" json:"id"`
	PolicyID          *uuid.UUID       `bun:",nullzero" json:"policyID"`
	PeriodStart       time.Time        `bun:",notnull" json:"periodStart"`
	PeriodEnd         time.Time        `bun:",notnull" json:"periodEnd"`
	TotalTransactions int64            `bun:",notnull" json:"totalTransactions"`
	MatchedCount      int64            `bun:",notnull" json:"matchedCount"`
	MatchRate         float64          `bun:",notnull" json:"matchRate"`
	AnomaliesByType   map[string]int64 `bun:"type:jsonb" json:"anomaliesByType"`
	GeneratedAt       time.Time        `bun:",notnull" json:"generatedAt"`
}
