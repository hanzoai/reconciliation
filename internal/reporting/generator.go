package reporting

import (
	"context"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
)

type Report struct {
	PolicyID          uuid.UUID        `json:"policyID"`
	PeriodStart       time.Time        `json:"periodStart"`
	PeriodEnd         time.Time        `json:"periodEnd"`
	TotalTransactions int64            `json:"totalTransactions"`
	MatchedCount      int64            `json:"matchedCount"`
	MatchRate         float64          `json:"matchRate"`
	AnomaliesByType   map[string]int64 `json:"anomaliesByType"`
	GeneratedAt       time.Time        `json:"generatedAt"`
}

type Generator struct {
	db *bun.DB
}

func NewGenerator(db *bun.DB) *Generator {
	return &Generator{db: db}
}

func (g *Generator) Generate(ctx context.Context, policyID uuid.UUID, from, to time.Time) (*Report, error) {
	totalTransactions, err := g.countTransactions(ctx, policyID, from, to)
	if err != nil {
		return nil, err
	}

	matchedCount, err := g.countMatchedTransactions(ctx, policyID, from, to)
	if err != nil {
		return nil, err
	}

	anomaliesByType, err := g.countAnomaliesByType(ctx, policyID, from, to)
	if err != nil {
		return nil, err
	}

	var matchRate float64
	if totalTransactions > 0 {
		matchRate = float64(matchedCount) / float64(totalTransactions)
	}

	return &Report{
		PolicyID:          policyID,
		PeriodStart:       from,
		PeriodEnd:         to,
		TotalTransactions: totalTransactions,
		MatchedCount:      matchedCount,
		MatchRate:         matchRate,
		AnomaliesByType:   anomaliesByType,
		GeneratedAt:       time.Now().UTC(),
	}, nil
}

func (g *Generator) countTransactions(ctx context.Context, policyID uuid.UUID, from, to time.Time) (int64, error) {
	// Since transactions are now stored in OpenSearch (not PostgreSQL),
	// we derive the total transaction count from the match table.
	// This counts all unique transaction IDs across all matches for the policy in the period.
	var result struct {
		Count int64 `bun:"count"`
	}

	err := g.db.NewRaw(`
		SELECT COALESCE(SUM(array_length(ledger_tx_ids, 1) + array_length(payment_tx_ids, 1)), 0) as count
		FROM reconciliations.match
		WHERE policy_id = ?
		AND created_at >= ?
		AND created_at <= ?
	`, policyID, from, to).Scan(ctx, &result)
	if err != nil {
		return 0, err
	}

	return result.Count, nil
}

func (g *Generator) countMatchedTransactions(ctx context.Context, policyID uuid.UUID, from, to time.Time) (int64, error) {
	// Count unique transactions that are part of matches created within the period
	// A match contains ledger_tx_ids and payment_tx_ids arrays
	// We count the total number of transaction IDs across all matches
	var result struct {
		Count int64 `bun:"count"`
	}

	err := g.db.NewRaw(`
		SELECT COALESCE(SUM(array_length(ledger_tx_ids, 1) + array_length(payment_tx_ids, 1)), 0) as count
		FROM reconciliations.match
		WHERE policy_id = ?
		AND decision IN (?, ?)
		AND created_at >= ?
		AND created_at <= ?
	`, policyID, models.DecisionMatched, models.DecisionManualMatch, from, to).Scan(ctx, &result)
	if err != nil {
		return 0, err
	}

	return result.Count, nil
}

func (g *Generator) countAnomaliesByType(ctx context.Context, policyID uuid.UUID, from, to time.Time) (map[string]int64, error) {
	var results []struct {
		Type  string `bun:"type"`
		Count int64  `bun:"count"`
	}

	err := g.db.NewSelect().
		Model((*models.Anomaly)(nil)).
		Column("type").
		ColumnExpr("COUNT(*) as count").
		Where("policy_id = ?", policyID).
		Where("created_at >= ?", from).
		Where("created_at <= ?", to).
		Group("type").
		Scan(ctx, &results)
	if err != nil {
		return nil, err
	}

	anomaliesByType := make(map[string]int64)
	for _, r := range results {
		anomaliesByType[r.Type] = r.Count
	}

	return anomaliesByType, nil
}
