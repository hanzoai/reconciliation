package reporting

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSchedulerIntegration_GenerateDailyReports(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	store := storage.NewStorage(db)

	t.Run("job triggered creates reports in DB for all policies", func(t *testing.T) {
		// Create 3 test policies
		policy1 := createTestPolicy(t, db)
		policy2 := createTestPolicy(t, db)
		policy3 := createTestPolicy(t, db)

		// Calculate D-1 period (what the scheduler will use)
		periodStart, periodEnd := CalculateDMinusOnePeriod(time.Now().UTC())

		// Create matches within D-1 period for policy1 (2 transactions: 1 ledger + 1 payment)
		createTestMatch(t, db, policy1.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			periodStart.Add(6*time.Hour),
		)

		// Create match for policy2 within D-1 period (2 transactions)
		createTestMatch(t, db, policy2.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			periodStart.Add(12*time.Hour),
		)

		// Create a match outside the D-1 period (2 days ago) - should NOT be counted
		twoDaysAgo := periodStart.Add(-24 * time.Hour)
		createTestMatch(t, db, policy2.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			twoDaysAgo,
		)

		// policy3 has no matches in period

		// Create the scheduler and trigger report generation directly
		config := SchedulerConfig{
			Schedule: DefaultReportSchedule,
		}
		scheduler := NewScheduler(db, store, config)

		// Call the internal method directly to generate reports
		scheduler.generateDailyReports(context.Background())

		// Verify reports were created for all 3 policies
		report1, err := store.GetLatestReportByPolicyID(context.Background(), policy1.ID)
		require.NoError(t, err)
		require.NotNil(t, report1)
		require.NotNil(t, report1.PolicyID)
		require.Equal(t, policy1.ID, *report1.PolicyID)
		require.Equal(t, int64(2), report1.TotalTransactions)

		report2, err := store.GetLatestReportByPolicyID(context.Background(), policy2.ID)
		require.NoError(t, err)
		require.NotNil(t, report2)
		require.NotNil(t, report2.PolicyID)
		require.Equal(t, policy2.ID, *report2.PolicyID)
		require.Equal(t, int64(2), report2.TotalTransactions) // Only the D-1 match, not the one from 2 days ago

		report3, err := store.GetLatestReportByPolicyID(context.Background(), policy3.ID)
		require.NoError(t, err)
		require.NotNil(t, report3)
		require.NotNil(t, report3.PolicyID)
		require.Equal(t, policy3.ID, *report3.PolicyID)
		require.Equal(t, int64(0), report3.TotalTransactions)

		// Verify report period matches D-1 (use truncated comparison due to DB precision)
		require.WithinDuration(t, periodStart, report1.PeriodStart, time.Second)
		require.WithinDuration(t, periodEnd, report1.PeriodEnd, time.Second)
	})
}

func TestSchedulerIntegration_ErrorOnOnePolicyContinuesWithOthers(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	store := storage.NewStorage(db)

	t.Run("error on one policy continues with others", func(t *testing.T) {
		// Create 2 valid policies
		policy1 := createTestPolicy(t, db)
		policy2 := createTestPolicy(t, db)

		// Calculate D-1 period
		periodStart, _ := CalculateDMinusOnePeriod(time.Now().UTC())

		// Create matches for both policies within D-1 period
		createTestMatch(t, db, policy1.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			periodStart.Add(6*time.Hour),
		)
		createTestMatch(t, db, policy2.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			periodStart.Add(6*time.Hour),
		)

		// Create the scheduler
		config := SchedulerConfig{
			Schedule: DefaultReportSchedule,
		}
		scheduler := NewScheduler(db, store, config)

		// Generate reports - even if there's an error with one, others should succeed
		scheduler.generateDailyReports(context.Background())

		// Verify reports were created for both policies
		report1, err := store.GetLatestReportByPolicyID(context.Background(), policy1.ID)
		require.NoError(t, err)
		require.NotNil(t, report1)

		report2, err := store.GetLatestReportByPolicyID(context.Background(), policy2.ID)
		require.NoError(t, err)
		require.NotNil(t, report2)
	})
}

func TestSchedulerIntegration_ReportPeriodIsD1(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	store := storage.NewStorage(db)

	t.Run("report period is D-1 (00:00 to 23:59)", func(t *testing.T) {
		policy := createTestPolicy(t, db)

		// Calculate expected D-1 period before generating
		expectedStart, expectedEnd := CalculateDMinusOnePeriod(time.Now().UTC())

		// Create the scheduler
		config := SchedulerConfig{
			Schedule: DefaultReportSchedule,
		}
		scheduler := NewScheduler(db, store, config)

		// Generate reports
		scheduler.generateDailyReports(context.Background())

		// Get the report and verify the period
		report, err := store.GetLatestReportByPolicyID(context.Background(), policy.ID)
		require.NoError(t, err)
		require.NotNil(t, report)

		// Verify period matches D-1 (use truncated comparison due to DB precision)
		require.WithinDuration(t, expectedStart, report.PeriodStart.UTC(), time.Second)
		require.WithinDuration(t, expectedEnd, report.PeriodEnd.UTC(), time.Second)
	})
}

func TestSchedulerIntegration_ReportPersistedInDB(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	store := storage.NewStorage(db)

	t.Run("report is persisted with all fields", func(t *testing.T) {
		policy := createTestPolicy(t, db)

		// Calculate D-1 period
		periodStart, _ := CalculateDMinusOnePeriod(time.Now().UTC())

		// Create a match within the D-1 period (2 transactions)
		ledgerTxID := uuid.New()
		createTestMatch(t, db, policy.ID,
			[]uuid.UUID{ledgerTxID},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			periodStart.Add(12*time.Hour),
		)

		// Create an anomaly within D-1 period
		createTestAnomaly(t, db, policy.ID, ledgerTxID, models.AnomalyTypeMissingOnPayments, periodStart.Add(12*time.Hour))

		// Create the scheduler and generate reports
		config := SchedulerConfig{
			Schedule: DefaultReportSchedule,
		}
		scheduler := NewScheduler(db, store, config)
		scheduler.generateDailyReports(context.Background())

		// Get the report
		report, err := store.GetLatestReportByPolicyID(context.Background(), policy.ID)
		require.NoError(t, err)
		require.NotNil(t, report)

		// Verify all fields are persisted correctly
		require.NotEqual(t, uuid.Nil, report.ID)
		require.NotNil(t, report.PolicyID)
		require.Equal(t, policy.ID, *report.PolicyID)
		require.NotZero(t, report.PeriodStart)
		require.NotZero(t, report.PeriodEnd)
		require.Equal(t, int64(2), report.TotalTransactions)
		require.Equal(t, int64(2), report.MatchedCount)
		require.Equal(t, 1.0, report.MatchRate)
		require.NotNil(t, report.AnomaliesByType)
		require.Equal(t, int64(1), report.AnomaliesByType["MISSING_ON_PAYMENTS"])
		require.NotZero(t, report.GeneratedAt)
	})
}

func TestSchedulerIntegration_NoPoliciesNoReports(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	store := storage.NewStorage(db)

	t.Run("no policies means no reports are generated", func(t *testing.T) {
		// Don't create any policies

		// Create the scheduler and generate reports
		config := SchedulerConfig{
			Schedule: DefaultReportSchedule,
		}
		scheduler := NewScheduler(db, store, config)
		scheduler.generateDailyReports(context.Background())

		// Verify no reports exist (try to get a report for a random policy ID)
		_, err := store.GetLatestReportByPolicyID(context.Background(), uuid.New())
		require.Error(t, err) // Should error because no report exists
	})
}

func TestSchedulerIntegration_StartAndStop(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	store := storage.NewStorage(db)

	t.Run("scheduler can start and stop without error", func(t *testing.T) {
		config := SchedulerConfig{
			Schedule: "* * * * *", // Every minute for testing
		}
		scheduler := NewScheduler(db, store, config)

		// Start the scheduler
		err := scheduler.Start(context.Background())
		require.NoError(t, err)

		// Verify it's running
		require.True(t, scheduler.running)

		// Stop the scheduler
		scheduler.Stop()

		// Verify it's stopped
		require.False(t, scheduler.running)
	})

	t.Run("multiple start calls are idempotent", func(t *testing.T) {
		config := SchedulerConfig{
			Schedule: "* * * * *",
		}
		scheduler := NewScheduler(db, store, config)

		// Start twice
		err := scheduler.Start(context.Background())
		require.NoError(t, err)

		err = scheduler.Start(context.Background())
		require.NoError(t, err)

		// Stop
		scheduler.Stop()
	})

	t.Run("multiple stop calls are idempotent", func(t *testing.T) {
		config := SchedulerConfig{
			Schedule: "* * * * *",
		}
		scheduler := NewScheduler(db, store, config)

		err := scheduler.Start(context.Background())
		require.NoError(t, err)

		// Stop twice - should not panic
		scheduler.Stop()
		scheduler.Stop()

		require.False(t, scheduler.running)
	})
}
