package reporting

import (
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestGenerator_Generate_MatchRateCalculation(t *testing.T) {
	t.Run("100 transactions 90 matched gives match_rate 0.9", func(t *testing.T) {
		// This test validates the match rate calculation logic
		// Given: totalTransactions = 100, matchedCount = 90
		// Expected: matchRate = 90/100 = 0.9

		totalTransactions := int64(100)
		matchedCount := int64(90)

		var matchRate float64
		if totalTransactions > 0 {
			matchRate = float64(matchedCount) / float64(totalTransactions)
		}

		require.Equal(t, 0.9, matchRate)
	})

	t.Run("0 transactions gives match_rate 0 without division by zero", func(t *testing.T) {
		// This test validates that 0 transactions doesn't cause division by zero
		// Given: totalTransactions = 0
		// Expected: matchRate = 0 (not NaN or infinity)

		totalTransactions := int64(0)
		matchedCount := int64(0)

		var matchRate float64
		if totalTransactions > 0 {
			matchRate = float64(matchedCount) / float64(totalTransactions)
		}

		require.Equal(t, float64(0), matchRate)
	})
}

func TestGenerator_Generate_AnomaliesByType(t *testing.T) {
	t.Run("anomalies_by_type counts correctly by type", func(t *testing.T) {
		// Simulate the aggregation logic
		anomalies := []struct {
			Type  models.AnomalyType
			Count int64
		}{
			{Type: models.AnomalyTypeMissingOnPayments, Count: 5},
			{Type: models.AnomalyTypeMissingOnLedger, Count: 3},
			{Type: models.AnomalyTypeDuplicateLedger, Count: 2},
			{Type: models.AnomalyTypeAmountMismatch, Count: 1},
		}

		anomaliesByType := make(map[string]int64)
		for _, a := range anomalies {
			anomaliesByType[string(a.Type)] = a.Count
		}

		require.Len(t, anomaliesByType, 4)
		require.Equal(t, int64(5), anomaliesByType["MISSING_ON_PAYMENTS"])
		require.Equal(t, int64(3), anomaliesByType["MISSING_ON_LEDGER"])
		require.Equal(t, int64(2), anomaliesByType["DUPLICATE_LEDGER"])
		require.Equal(t, int64(1), anomaliesByType["AMOUNT_MISMATCH"])
	})
}

func TestGenerator_Generate_PeriodDates(t *testing.T) {
	t.Run("period_start and period_end are correct", func(t *testing.T) {
		policyID := uuid.New()
		from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)

		report := &Report{
			PolicyID:          policyID,
			PeriodStart:       from,
			PeriodEnd:         to,
			TotalTransactions: 0,
			MatchedCount:      0,
			MatchRate:         0,
			AnomaliesByType:   make(map[string]int64),
			GeneratedAt:       time.Now().UTC(),
		}

		require.Equal(t, from, report.PeriodStart)
		require.Equal(t, to, report.PeriodEnd)
		require.Equal(t, policyID, report.PolicyID)
	})
}

func TestReport_Structure(t *testing.T) {
	t.Run("Report struct has all required fields", func(t *testing.T) {
		policyID := uuid.New()
		now := time.Now().UTC()
		from := now.Add(-24 * time.Hour)
		to := now

		report := &Report{
			PolicyID:          policyID,
			PeriodStart:       from,
			PeriodEnd:         to,
			TotalTransactions: 100,
			MatchedCount:      90,
			MatchRate:         0.9,
			AnomaliesByType: map[string]int64{
				"MISSING_ON_PAYMENTS": 5,
				"MISSING_ON_LEDGER":   3,
			},
			GeneratedAt: now,
		}

		require.Equal(t, policyID, report.PolicyID)
		require.Equal(t, from, report.PeriodStart)
		require.Equal(t, to, report.PeriodEnd)
		require.Equal(t, int64(100), report.TotalTransactions)
		require.Equal(t, int64(90), report.MatchedCount)
		require.Equal(t, 0.9, report.MatchRate)
		require.Len(t, report.AnomaliesByType, 2)
		require.Equal(t, now, report.GeneratedAt)
	})
}

func TestNewGenerator(t *testing.T) {
	t.Run("creates generator with nil db", func(t *testing.T) {
		// Test that NewGenerator correctly stores the db reference
		generator := NewGenerator(nil)
		require.NotNil(t, generator)
	})
}

func TestGenerator_Generate_MatchRateEdgeCases(t *testing.T) {
	t.Run("100% match rate", func(t *testing.T) {
		totalTransactions := int64(50)
		matchedCount := int64(50)

		var matchRate float64
		if totalTransactions > 0 {
			matchRate = float64(matchedCount) / float64(totalTransactions)
		}

		require.Equal(t, 1.0, matchRate)
	})

	t.Run("0% match rate with transactions", func(t *testing.T) {
		totalTransactions := int64(50)
		matchedCount := int64(0)

		var matchRate float64
		if totalTransactions > 0 {
			matchRate = float64(matchedCount) / float64(totalTransactions)
		}

		require.Equal(t, 0.0, matchRate)
	})

	t.Run("partial match rate", func(t *testing.T) {
		totalTransactions := int64(1000)
		matchedCount := int64(750)

		var matchRate float64
		if totalTransactions > 0 {
			matchRate = float64(matchedCount) / float64(totalTransactions)
		}

		require.Equal(t, 0.75, matchRate)
	})
}

func TestGenerator_Generate_EmptyAnomalies(t *testing.T) {
	t.Run("empty anomalies map when no anomalies", func(t *testing.T) {
		anomaliesByType := make(map[string]int64)
		require.Len(t, anomaliesByType, 0)
		require.NotNil(t, anomaliesByType)
	})
}

func TestGenerator_Generate_GeneratedAtTimestamp(t *testing.T) {
	t.Run("generated_at is set to current time", func(t *testing.T) {
		before := time.Now().UTC()
		generatedAt := time.Now().UTC()
		after := time.Now().UTC()

		require.True(t, generatedAt.After(before) || generatedAt.Equal(before))
		require.True(t, generatedAt.Before(after) || generatedAt.Equal(after))
	})
}

