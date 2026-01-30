package reporting

import (
	"context"
	"crypto/rand"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/utils"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage/migrations"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

var (
	srv   *pgtesting.PostgresServer
	bunDB *bun.DB
)

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()), pgtesting.WithVersion("17"))

		db, err := sql.Open("pgx", srv.GetDSN())
		if err != nil {
			logging.Error(err)
			os.Exit(1)
		}

		bunDB = bun.NewDB(db, pgdialect.New())

		return m.Run()
	})
}

func newTestDB(t *testing.T) *bun.DB {
	t.Helper()
	ctx := logging.TestingContext()

	pgServer := srv.NewDatabase(t)

	db, err := bunconnect.OpenSQLDB(ctx, pgServer.ConnectionOptions())
	require.NoError(t, err)

	key := make([]byte, 64)
	_, err = rand.Read(key)
	require.NoError(t, err)

	err = migrations.Migrate(context.Background(), db)
	require.NoError(t, err)

	return db
}

func createTestPolicy(t *testing.T, db *bun.DB) *models.Policy {
	t.Helper()
	policy := &models.Policy{
		ID:         uuid.New(),
		CreatedAt:  time.Now().UTC(),
		Name:       "test-policy-" + uuid.New().String()[:8],
		LedgerName: "default",
		LedgerQuery: map[string]interface{}{
			"$match": map[string]interface{}{
				"account": "test",
			},
		},
		PaymentsPoolID:      uuid.New(),
		Mode:                "transactional", // Required for reporting to include this policy
		Topology:            "1:1",
		DeterministicFields: []string{"external_id"},
	}

	_, err := db.NewInsert().Model(policy).Exec(context.Background())
	require.NoError(t, err)

	return policy
}

func createTestMatch(t *testing.T, db *bun.DB, policyID uuid.UUID, ledgerTxIDs, paymentTxIDs []uuid.UUID, decision models.Decision, createdAt time.Time) *models.Match {
	t.Helper()
	match := &models.Match{
		ID:                     uuid.New(),
		PolicyID:               &policyID,
		LedgerTransactionIDs:   ledgerTxIDs,
		PaymentsTransactionIDs: paymentTxIDs,
		Score:                  0.95,
		Decision:               decision,
		Explanation: models.Explanation{
			Reason: "Test match",
		},
		CreatedAt: createdAt,
	}

	_, err := db.NewInsert().Model(match).Exec(context.Background())
	require.NoError(t, err)

	return match
}

func createTestAnomaly(t *testing.T, db *bun.DB, policyID, txID uuid.UUID, anomalyType models.AnomalyType, createdAt time.Time) *models.Anomaly {
	t.Helper()
	anomaly := &models.Anomaly{
		ID:            uuid.New(),
		PolicyID:      &policyID,
		TransactionID: &txID,
		Type:          anomalyType,
		Severity:      models.SeverityHigh,
		State:         models.AnomalyStateOpen,
		Reason:        "Test anomaly",
		CreatedAt:     createdAt,
	}

	_, err := db.NewInsert().Model(anomaly).Exec(context.Background())
	require.NoError(t, err)

	return anomaly
}

func TestGeneratorIntegration_Generate(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	generator := NewGenerator(db)
	policy := createTestPolicy(t, db)

	// Define time range for the test
	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now.Add(1 * time.Hour)

	t.Run("50 matches with 45 MATCHED gives correct counts", func(t *testing.T) {
		// Create 45 MATCHED matches (90 matched transactions: 45 ledger + 45 payments)
		for i := 0; i < 45; i++ {
			createTestMatch(t, db, policy.ID,
				[]uuid.UUID{uuid.New()},
				[]uuid.UUID{uuid.New()},
				models.DecisionMatched,
				now.Add(-6*time.Hour),
			)
		}

		// Create 5 UNMATCHED matches (10 unmatched transactions: 5 ledger + 5 payments)
		for i := 0; i < 5; i++ {
			createTestMatch(t, db, policy.ID,
				[]uuid.UUID{uuid.New()},
				[]uuid.UUID{uuid.New()},
				models.DecisionUnmatched,
				now.Add(-6*time.Hour),
			)
		}

		report, err := generator.Generate(context.Background(), policy.ID, from, to)
		require.NoError(t, err)
		require.NotNil(t, report)

		require.Equal(t, int64(100), report.TotalTransactions)
		require.Equal(t, int64(90), report.MatchedCount)
		require.Equal(t, 0.9, report.MatchRate)
	})
}

func TestGeneratorIntegration_Generate_ZeroTransactions(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	generator := NewGenerator(db)
	policy := createTestPolicy(t, db)

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now.Add(1 * time.Hour)

	t.Run("0 transactions gives match_rate 0 without division by zero", func(t *testing.T) {
		// Don't create any matches

		report, err := generator.Generate(context.Background(), policy.ID, from, to)
		require.NoError(t, err)
		require.NotNil(t, report)

		require.Equal(t, int64(0), report.TotalTransactions)
		require.Equal(t, int64(0), report.MatchedCount)
		require.Equal(t, float64(0), report.MatchRate)
	})
}

func TestGeneratorIntegration_Generate_AnomaliesByType(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	generator := NewGenerator(db)
	policy := createTestPolicy(t, db)

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now.Add(1 * time.Hour)

	t.Run("anomalies_by_type counts correctly by type", func(t *testing.T) {
		txID := uuid.New()

		// Create anomalies of different types
		for i := 0; i < 5; i++ {
			createTestAnomaly(t, db, policy.ID, txID, models.AnomalyTypeMissingOnPayments, now.Add(-6*time.Hour))
		}
		for i := 0; i < 3; i++ {
			createTestAnomaly(t, db, policy.ID, txID, models.AnomalyTypeMissingOnLedger, now.Add(-6*time.Hour))
		}
		for i := 0; i < 2; i++ {
			createTestAnomaly(t, db, policy.ID, txID, models.AnomalyTypeDuplicateLedger, now.Add(-6*time.Hour))
		}
		createTestAnomaly(t, db, policy.ID, txID, models.AnomalyTypeAmountMismatch, now.Add(-6*time.Hour))

		report, err := generator.Generate(context.Background(), policy.ID, from, to)
		require.NoError(t, err)
		require.NotNil(t, report)

		require.Len(t, report.AnomaliesByType, 4)
		require.Equal(t, int64(5), report.AnomaliesByType["MISSING_ON_PAYMENTS"])
		require.Equal(t, int64(3), report.AnomaliesByType["MISSING_ON_LEDGER"])
		require.Equal(t, int64(2), report.AnomaliesByType["DUPLICATE_LEDGER"])
		require.Equal(t, int64(1), report.AnomaliesByType["AMOUNT_MISMATCH"])
	})
}

func TestGeneratorIntegration_Generate_PeriodDates(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	generator := NewGenerator(db)
	policy := createTestPolicy(t, db)

	t.Run("period_start and period_end are correct", func(t *testing.T) {
		from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		to := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)

		report, err := generator.Generate(context.Background(), policy.ID, from, to)
		require.NoError(t, err)
		require.NotNil(t, report)

		require.Equal(t, from, report.PeriodStart)
		require.Equal(t, to, report.PeriodEnd)
		require.Equal(t, policy.ID, report.PolicyID)
	})
}

func TestGeneratorIntegration_Generate_OnlyCountsWithinPeriod(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	generator := NewGenerator(db)
	policy := createTestPolicy(t, db)

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now

	t.Run("only counts matches within period", func(t *testing.T) {
		// Create match inside period (2 transactions)
		createTestMatch(t, db, policy.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			now.Add(-12*time.Hour),
		)

		// Create match outside period - before (2 transactions)
		createTestMatch(t, db, policy.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			now.Add(-48*time.Hour),
		)

		// Create match outside period - after (2 transactions)
		createTestMatch(t, db, policy.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			now.Add(24*time.Hour),
		)

		report, err := generator.Generate(context.Background(), policy.ID, from, to)
		require.NoError(t, err)
		require.NotNil(t, report)

		require.Equal(t, int64(2), report.TotalTransactions)
	})
}

func TestGeneratorIntegration_Generate_ManualMatchCountsAsMatched(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	generator := NewGenerator(db)
	policy := createTestPolicy(t, db)

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now.Add(1 * time.Hour)

	t.Run("MANUAL_MATCH decision counts as matched", func(t *testing.T) {
		// Create 1 MATCHED match (2 transactions)
		createTestMatch(t, db, policy.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionMatched,
			now.Add(-6*time.Hour),
		)

		// Create 1 MANUAL_MATCH (2 transactions)
		createTestMatch(t, db, policy.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionManualMatch,
			now.Add(-6*time.Hour),
		)

		report, err := generator.Generate(context.Background(), policy.ID, from, to)
		require.NoError(t, err)
		require.NotNil(t, report)

		require.Equal(t, int64(4), report.TotalTransactions)
		require.Equal(t, int64(4), report.MatchedCount) // Both matches count
		require.Equal(t, 1.0, report.MatchRate)
	})
}

func TestGeneratorIntegration_Generate_UnmatchedDoesNotCount(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	generator := NewGenerator(db)
	policy := createTestPolicy(t, db)

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now.Add(1 * time.Hour)

	t.Run("UNMATCHED decision does not count as matched", func(t *testing.T) {
		// Create 1 UNMATCHED match (2 transactions)
		createTestMatch(t, db, policy.ID,
			[]uuid.UUID{uuid.New()},
			[]uuid.UUID{uuid.New()},
			models.DecisionUnmatched,
			now.Add(-6*time.Hour),
		)

		report, err := generator.Generate(context.Background(), policy.ID, from, to)
		require.NoError(t, err)
		require.NotNil(t, report)

		require.Equal(t, int64(2), report.TotalTransactions)
		require.Equal(t, int64(0), report.MatchedCount) // UNMATCHED doesn't count
		require.Equal(t, 0.0, report.MatchRate)
	})
}

func TestGeneratorIntegration_Generate_GeneratedAtIsSet(t *testing.T) {
	db := newTestDB(t)
	defer func() { _ = db.Close() }()

	generator := NewGenerator(db)
	policy := createTestPolicy(t, db)

	now := time.Now().UTC()
	from := now.Add(-24 * time.Hour)
	to := now

	t.Run("generated_at is set to current time", func(t *testing.T) {
		before := time.Now().UTC()

		report, err := generator.Generate(context.Background(), policy.ID, from, to)
		require.NoError(t, err)
		require.NotNil(t, report)

		after := time.Now().UTC()

		require.True(t, report.GeneratedAt.After(before) || report.GeneratedAt.Equal(before))
		require.True(t, report.GeneratedAt.Before(after) || report.GeneratedAt.Equal(after))
	})
}
