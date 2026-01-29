package migrations

import (
	"context"
	"database/sql"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/utils"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

var srv *pgtesting.PostgresServer

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()), pgtesting.WithVersion("17"))
		return m.Run()
	})
}

func TestMigrationUpDown(t *testing.T) {
	ctx := context.Background()

	pgServer := srv.NewDatabase(t)

	db, err := sql.Open("pgx", pgServer.ConnString())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	bunDB := bun.NewDB(db, pgdialect.New())

	// Test migration up
	err = Migrate(ctx, bunDB)
	require.NoError(t, err, "migration up should succeed")

	// Verify transaction table exists with correct structure
	var exists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'transaction'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "transaction table should exist after migration up")

	// Verify columns exist
	columns := []string{"id", "policy_id", "side", "provider", "external_id", "amount", "currency", "occurred_at", "ingested_at", "metadata"}
	for _, col := range columns {
		var colExists bool
		err = bunDB.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.columns
				WHERE table_schema = 'reconciliations'
				AND table_name = 'transaction'
				AND column_name = ?
			)
		`, col).Scan(&colExists)
		require.NoError(t, err)
		require.True(t, colExists, "column %s should exist", col)
	}

	// Verify UNIQUE constraint on (side, external_id) - after migration #18, policy_id is not part of unique constraint
	var constraintExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.table_constraints
			WHERE table_schema = 'reconciliations'
			AND table_name = 'transaction'
			AND constraint_name = 'transaction_side_external_id_unique'
			AND constraint_type = 'UNIQUE'
		)
	`).Scan(&constraintExists)
	require.NoError(t, err)
	require.True(t, constraintExists, "UNIQUE constraint on (side, external_id) should exist")

	// Verify indexes exist
	indexes := []string{"transaction_policy_id_side_idx", "transaction_external_id_idx"}
	for _, idx := range indexes {
		var indexExists bool
		err = bunDB.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM pg_indexes
				WHERE schemaname = 'reconciliations'
				AND tablename = 'transaction'
				AND indexname = ?
			)
		`, idx).Scan(&indexExists)
		require.NoError(t, err)
		require.True(t, indexExists, "index %s should exist", idx)
	}

	// Verify foreign key constraint was removed after migration #18 (policy_id is now nullable)
	var fkExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.table_constraints
			WHERE table_schema = 'reconciliations'
			AND table_name = 'transaction'
			AND constraint_name = 'transaction_policy_fk'
			AND constraint_type = 'FOREIGN KEY'
		)
	`).Scan(&fkExists)
	require.NoError(t, err)
	require.False(t, fkExists, "foreign key constraint should not exist after migration #18")

	// Test migration down
	err = DownTransaction(ctx, bunDB)
	require.NoError(t, err, "migration down should succeed")

	// Verify transaction table no longer exists
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'transaction'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "transaction table should not exist after migration down")

	// Verify we can re-create the table manually (proving the down migration is reversible)
	// Note: after migration #18, policy_id is nullable and unique constraint is on (side, external_id)
	_, err = bunDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS reconciliations.transaction (
			id uuid NOT NULL,
			policy_id uuid,
			side varchar(10) NOT NULL,
			provider varchar(100) NOT NULL,
			external_id varchar(255) NOT NULL,
			amount bigint NOT NULL,
			currency varchar(3) NOT NULL,
			occurred_at timestamp with time zone NOT NULL,
			ingested_at timestamp with time zone NOT NULL,
			metadata jsonb,
			CONSTRAINT transaction_pk PRIMARY KEY (id),
			CONSTRAINT transaction_side_external_id_unique UNIQUE (side, external_id)
		)
	`)
	require.NoError(t, err, "should be able to recreate table after down migration")

	// Verify table exists again
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'transaction'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "transaction table should exist after re-creating")
}

func TestMatchMigrationUpDown(t *testing.T) {
	ctx := context.Background()

	pgServer := srv.NewDatabase(t)

	db, err := sql.Open("pgx", pgServer.ConnString())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	bunDB := bun.NewDB(db, pgdialect.New())

	// Test migration up
	err = Migrate(ctx, bunDB)
	require.NoError(t, err, "migration up should succeed")

	// Verify match table exists with correct structure
	var exists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'match'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "match table should exist after migration up")

	// Verify columns exist
	columns := []string{"id", "policy_id", "ledger_tx_ids", "payment_tx_ids", "score", "decision", "explanation", "created_at"}
	for _, col := range columns {
		var colExists bool
		err = bunDB.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.columns
				WHERE table_schema = 'reconciliations'
				AND table_name = 'match'
				AND column_name = ?
			)
		`, col).Scan(&colExists)
		require.NoError(t, err)
		require.True(t, colExists, "column %s should exist", col)
	}

	// Verify column types
	var dataType string
	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'match'
		AND column_name = 'ledger_tx_ids'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "ARRAY", dataType, "ledger_tx_ids should be an array type")

	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'match'
		AND column_name = 'payment_tx_ids'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "ARRAY", dataType, "payment_tx_ids should be an array type")

	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'match'
		AND column_name = 'score'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "numeric", dataType, "score should be numeric/decimal type")

	// Verify index on (policy_id, decision)
	var indexExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM pg_indexes
			WHERE schemaname = 'reconciliations'
			AND tablename = 'match'
			AND indexname = 'match_policy_id_decision_idx'
		)
	`).Scan(&indexExists)
	require.NoError(t, err)
	require.True(t, indexExists, "index match_policy_id_decision_idx should exist")

	// Verify foreign key constraint
	var fkExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.table_constraints
			WHERE table_schema = 'reconciliations'
			AND table_name = 'match'
			AND constraint_name = 'match_policy_fk'
			AND constraint_type = 'FOREIGN KEY'
		)
	`).Scan(&fkExists)
	require.NoError(t, err)
	require.True(t, fkExists, "foreign key constraint should exist")

	// Test migration down
	err = DownMatch(ctx, bunDB)
	require.NoError(t, err, "migration down should succeed")

	// Verify match table no longer exists
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'match'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "match table should not exist after migration down")

	// Verify we can re-create the table manually (proving the down migration is reversible)
	_, err = bunDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS reconciliations.match (
			id uuid NOT NULL,
			policy_id uuid NOT NULL,
			ledger_tx_ids uuid[] NOT NULL,
			payment_tx_ids uuid[] NOT NULL,
			score decimal(5,4) NOT NULL,
			decision varchar(30) NOT NULL,
			explanation jsonb,
			created_at timestamp with time zone NOT NULL,
			CONSTRAINT match_pk PRIMARY KEY (id)
		)
	`)
	require.NoError(t, err, "should be able to recreate table after down migration")

	// Verify table exists again
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'match'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "match table should exist after re-creating")
}

func TestAnomalyMigrationUpDown(t *testing.T) {
	ctx := context.Background()

	pgServer := srv.NewDatabase(t)

	db, err := sql.Open("pgx", pgServer.ConnString())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	bunDB := bun.NewDB(db, pgdialect.New())

	// Test migration up
	err = Migrate(ctx, bunDB)
	require.NoError(t, err, "migration up should succeed")

	// Verify anomaly table exists with correct structure
	var exists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'anomaly'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "anomaly table should exist after migration up")

	// Verify columns exist
	columns := []string{"id", "policy_id", "transaction_id", "type", "severity", "state", "reason", "created_at", "resolved_at"}
	for _, col := range columns {
		var colExists bool
		err = bunDB.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT FROM information_schema.columns
				WHERE table_schema = 'reconciliations'
				AND table_name = 'anomaly'
				AND column_name = ?
			)
		`, col).Scan(&colExists)
		require.NoError(t, err)
		require.True(t, colExists, "column %s should exist", col)
	}

	// Verify column types and constraints
	var dataType string
	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'anomaly'
		AND column_name = 'type'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "character varying", dataType, "type should be varchar")

	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'anomaly'
		AND column_name = 'severity'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "character varying", dataType, "severity should be varchar")

	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'anomaly'
		AND column_name = 'state'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "character varying", dataType, "state should be varchar")

	// Verify state column has default value 'open'
	var columnDefault sql.NullString
	err = bunDB.QueryRowContext(ctx, `
		SELECT column_default FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'anomaly'
		AND column_name = 'state'
	`).Scan(&columnDefault)
	require.NoError(t, err)
	require.True(t, columnDefault.Valid, "state column should have a default value")
	require.Contains(t, columnDefault.String, "open", "state column default should be 'open'")

	// Verify index on (policy_id, state)
	var indexExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM pg_indexes
			WHERE schemaname = 'reconciliations'
			AND tablename = 'anomaly'
			AND indexname = 'anomaly_policy_id_state_idx'
		)
	`).Scan(&indexExists)
	require.NoError(t, err)
	require.True(t, indexExists, "index anomaly_policy_id_state_idx should exist")

	// Verify foreign key constraint to policy
	var fkPolicyExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.table_constraints
			WHERE table_schema = 'reconciliations'
			AND table_name = 'anomaly'
			AND constraint_name = 'anomaly_policy_fk'
			AND constraint_type = 'FOREIGN KEY'
		)
	`).Scan(&fkPolicyExists)
	require.NoError(t, err)
	require.True(t, fkPolicyExists, "foreign key constraint to policy should exist")

	// Verify foreign key constraint to transaction
	var fkTransactionExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.table_constraints
			WHERE table_schema = 'reconciliations'
			AND table_name = 'anomaly'
			AND constraint_name = 'anomaly_transaction_fk'
			AND constraint_type = 'FOREIGN KEY'
		)
	`).Scan(&fkTransactionExists)
	require.NoError(t, err)
	require.True(t, fkTransactionExists, "foreign key constraint to transaction should exist")

	// Test migration down
	err = DownAnomaly(ctx, bunDB)
	require.NoError(t, err, "migration down should succeed")

	// Verify anomaly table no longer exists
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'anomaly'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.False(t, exists, "anomaly table should not exist after migration down")

	// Verify we can re-create the table manually (proving the down migration is reversible)
	_, err = bunDB.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS reconciliations.anomaly (
			id uuid NOT NULL,
			policy_id uuid NOT NULL,
			transaction_id uuid NOT NULL,
			type varchar(50) NOT NULL,
			severity varchar(20) NOT NULL,
			state varchar(20) NOT NULL DEFAULT 'open',
			reason text,
			created_at timestamp with time zone NOT NULL,
			resolved_at timestamp with time zone,
			CONSTRAINT anomaly_pk PRIMARY KEY (id)
		)
	`)
	require.NoError(t, err, "should be able to recreate table after down migration")

	// Verify table exists again
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'anomaly'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "anomaly table should exist after re-creating")
}

func TestPolicyModeMigrationUpDown(t *testing.T) {
	ctx := context.Background()

	pgServer := srv.NewDatabase(t)

	db, err := sql.Open("pgx", pgServer.ConnString())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	bunDB := bun.NewDB(db, pgdialect.New())

	// First, run migrations up to migration #6 (before mode column)
	// We simulate this by running all migrations and then checking the mode column behavior
	err = Migrate(ctx, bunDB)
	require.NoError(t, err, "migration up should succeed")

	// Verify policy table exists
	var exists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "policy table should exist after migration up")

	// Verify mode column exists
	var colExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'policy_mode'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.True(t, colExists, "policy_mode column should exist")

	// Verify column type is varchar(20)
	var dataType string
	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'policy'
		AND column_name = 'policy_mode'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "character varying", dataType, "mode should be varchar type")

	// Verify mode column has default value 'balance'
	var columnDefault sql.NullString
	err = bunDB.QueryRowContext(ctx, `
		SELECT column_default FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'policy'
		AND column_name = 'policy_mode'
	`).Scan(&columnDefault)
	require.NoError(t, err)
	require.True(t, columnDefault.Valid, "mode column should have a default value")
	require.Contains(t, columnDefault.String, "balance", "mode column default should be 'balance'")

	// Insert a policy without specifying mode to verify default value works
	_, err = bunDB.ExecContext(ctx, `
		INSERT INTO reconciliations.policy (id, created_at, name, ledger_name, ledger_query, payments_pool_id)
		VALUES ('123e4567-e89b-12d3-a456-426614174000', NOW(), 'Test Policy', 'test-ledger', '{}', '123e4567-e89b-12d3-a456-426614174001')
	`)
	require.NoError(t, err, "should be able to insert policy without specifying mode")

	// Verify the inserted policy has mode = 'balance'
	var mode string
	err = bunDB.QueryRowContext(ctx, `
		SELECT policy_mode FROM reconciliations.policy WHERE id = '123e4567-e89b-12d3-a456-426614174000'
	`).Scan(&mode)
	require.NoError(t, err)
	require.Equal(t, "balance", mode, "existing policies should have mode = 'balance'")

	// Verify we can insert a policy with mode = 'transactional'
	_, err = bunDB.ExecContext(ctx, `
		INSERT INTO reconciliations.policy (id, created_at, name, ledger_name, ledger_query, payments_pool_id, policy_mode)
		VALUES ('123e4567-e89b-12d3-a456-426614174002', NOW(), 'Transactional Policy', 'test-ledger', '{}', '123e4567-e89b-12d3-a456-426614174001', 'transactional')
	`)
	require.NoError(t, err, "should be able to insert policy with mode = 'transactional'")

	// Verify the transactional policy has correct mode
	err = bunDB.QueryRowContext(ctx, `
		SELECT policy_mode FROM reconciliations.policy WHERE id = '123e4567-e89b-12d3-a456-426614174002'
	`).Scan(&mode)
	require.NoError(t, err)
	require.Equal(t, "transactional", mode, "policy should have mode = 'transactional'")

	// Test migration down
	err = DownPolicyMode(ctx, bunDB)
	require.NoError(t, err, "migration down should succeed")

	// Verify mode column no longer exists
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'policy_mode'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.False(t, colExists, "mode column should not exist after migration down")

	// Verify we can re-add the column manually (proving the down migration is reversible)
	_, err = bunDB.ExecContext(ctx, `
		ALTER TABLE reconciliations.policy ADD COLUMN policy_mode varchar(20) NOT NULL DEFAULT 'balance'
	`)
	require.NoError(t, err, "should be able to re-add mode column after down migration")

	// Verify column exists again
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'policy_mode'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.True(t, colExists, "policy_mode column should exist after re-adding")
}

func TestPolicyDeterministicFieldsMigrationUpDown(t *testing.T) {
	ctx := context.Background()

	pgServer := srv.NewDatabase(t)

	db, err := sql.Open("pgx", pgServer.ConnString())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	bunDB := bun.NewDB(db, pgdialect.New())

	// Run all migrations
	err = Migrate(ctx, bunDB)
	require.NoError(t, err, "migration up should succeed")

	// Verify policy table exists
	var exists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "policy table should exist after migration up")

	// Verify deterministic_fields column exists
	var colExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'deterministic_fields'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.True(t, colExists, "deterministic_fields column should exist")

	// Verify column type is text array
	var dataType string
	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'policy'
		AND column_name = 'deterministic_fields'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "ARRAY", dataType, "deterministic_fields should be an array type")

	// Verify column allows NULL (no default constraint)
	var isNullable string
	err = bunDB.QueryRowContext(ctx, `
		SELECT is_nullable FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'policy'
		AND column_name = 'deterministic_fields'
	`).Scan(&isNullable)
	require.NoError(t, err)
	require.Equal(t, "YES", isNullable, "deterministic_fields column should be nullable")

	// Insert a policy without specifying deterministic_fields to verify NULL default works
	_, err = bunDB.ExecContext(ctx, `
		INSERT INTO reconciliations.policy (id, created_at, name, ledger_name, ledger_query, payments_pool_id)
		VALUES ('223e4567-e89b-12d3-a456-426614174000', NOW(), 'Test Policy', 'test-ledger', '{}', '223e4567-e89b-12d3-a456-426614174001')
	`)
	require.NoError(t, err, "should be able to insert policy without specifying deterministic_fields")

	// Verify the inserted policy has deterministic_fields = NULL
	var deterministicFields sql.NullString
	err = bunDB.QueryRowContext(ctx, `
		SELECT deterministic_fields FROM reconciliations.policy WHERE id = '223e4567-e89b-12d3-a456-426614174000'
	`).Scan(&deterministicFields)
	require.NoError(t, err)
	require.False(t, deterministicFields.Valid, "deterministic_fields should be NULL for policies without explicit value")

	// Verify we can insert a policy with deterministic_fields = ['external_id', 'amount']
	_, err = bunDB.ExecContext(ctx, `
		INSERT INTO reconciliations.policy (id, created_at, name, ledger_name, ledger_query, payments_pool_id, deterministic_fields)
		VALUES ('223e4567-e89b-12d3-a456-426614174002', NOW(), 'Custom Fields Policy', 'test-ledger', '{}', '223e4567-e89b-12d3-a456-426614174001', ARRAY['external_id', 'amount'])
	`)
	require.NoError(t, err, "should be able to insert policy with custom deterministic_fields")

	// Verify the custom policy has correct deterministic_fields
	var customFields string
	err = bunDB.QueryRowContext(ctx, `
		SELECT deterministic_fields::text FROM reconciliations.policy WHERE id = '223e4567-e89b-12d3-a456-426614174002'
	`).Scan(&customFields)
	require.NoError(t, err)
	require.Equal(t, "{external_id,amount}", customFields, "policy should have correct deterministic_fields")

	// Test migration down
	err = DownPolicyDeterministicFields(ctx, bunDB)
	require.NoError(t, err, "migration down should succeed")

	// Verify deterministic_fields column no longer exists
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'deterministic_fields'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.False(t, colExists, "deterministic_fields column should not exist after migration down")

	// Verify we can re-add the column manually (proving the down migration is reversible)
	_, err = bunDB.ExecContext(ctx, `
		ALTER TABLE reconciliations.policy ADD COLUMN deterministic_fields TEXT[]
	`)
	require.NoError(t, err, "should be able to re-add deterministic_fields column after down migration")

	// Verify column exists again
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'deterministic_fields'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.True(t, colExists, "deterministic_fields column should exist after re-adding")
}

func TestPolicyScoringConfigMigrationUpDown(t *testing.T) {
	ctx := context.Background()

	pgServer := srv.NewDatabase(t)

	db, err := sql.Open("pgx", pgServer.ConnString())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	bunDB := bun.NewDB(db, pgdialect.New())

	// Run all migrations
	err = Migrate(ctx, bunDB)
	require.NoError(t, err, "migration up should succeed")

	// Verify policy table exists
	var exists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "policy table should exist after migration up")

	// Verify scoring_config column exists
	var colExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'scoring_config'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.True(t, colExists, "scoring_config column should exist")

	// Verify column type is jsonb
	var dataType string
	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'policy'
		AND column_name = 'scoring_config'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "jsonb", dataType, "scoring_config should be jsonb type")

	// Verify column allows NULL (no default constraint)
	var isNullable string
	err = bunDB.QueryRowContext(ctx, `
		SELECT is_nullable FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'policy'
		AND column_name = 'scoring_config'
	`).Scan(&isNullable)
	require.NoError(t, err)
	require.Equal(t, "YES", isNullable, "scoring_config column should be nullable")

	// Insert a policy without specifying scoring_config to verify NULL default works
	_, err = bunDB.ExecContext(ctx, `
		INSERT INTO reconciliations.policy (id, created_at, name, ledger_name, ledger_query, payments_pool_id)
		VALUES ('323e4567-e89b-12d3-a456-426614174000', NOW(), 'Test Policy', 'test-ledger', '{}', '323e4567-e89b-12d3-a456-426614174001')
	`)
	require.NoError(t, err, "should be able to insert policy without specifying scoring_config")

	// Verify the inserted policy has scoring_config = NULL
	var scoringConfig sql.NullString
	err = bunDB.QueryRowContext(ctx, `
		SELECT scoring_config FROM reconciliations.policy WHERE id = '323e4567-e89b-12d3-a456-426614174000'
	`).Scan(&scoringConfig)
	require.NoError(t, err)
	require.False(t, scoringConfig.Valid, "scoring_config should be NULL for policies without explicit value")

	// Verify we can insert a policy with scoring_config containing scoring parameters
	_, err = bunDB.ExecContext(ctx, `
		INSERT INTO reconciliations.policy (id, created_at, name, ledger_name, ledger_query, payments_pool_id, scoring_config)
		VALUES ('323e4567-e89b-12d3-a456-426614174002', NOW(), 'Custom Scoring Policy', 'test-ledger', '{}', '323e4567-e89b-12d3-a456-426614174001', '{"threshold": 0.85, "weights": {"amount": 0.4, "date": 0.3, "reference": 0.3}}')
	`)
	require.NoError(t, err, "should be able to insert policy with custom scoring_config")

	// Verify the custom policy has correct scoring_config
	var customConfig string
	err = bunDB.QueryRowContext(ctx, `
		SELECT scoring_config::text FROM reconciliations.policy WHERE id = '323e4567-e89b-12d3-a456-426614174002'
	`).Scan(&customConfig)
	require.NoError(t, err)
	require.Contains(t, customConfig, "threshold", "policy should have correct scoring_config with threshold")
	require.Contains(t, customConfig, "weights", "policy should have correct scoring_config with weights")

	// Test migration down
	err = DownPolicyScoringConfig(ctx, bunDB)
	require.NoError(t, err, "migration down should succeed")

	// Verify scoring_config column no longer exists
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'scoring_config'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.False(t, colExists, "scoring_config column should not exist after migration down")

	// Verify we can re-add the column manually (proving the down migration is reversible)
	_, err = bunDB.ExecContext(ctx, `
		ALTER TABLE reconciliations.policy ADD COLUMN scoring_config JSONB
	`)
	require.NoError(t, err, "should be able to re-add scoring_config column after down migration")

	// Verify column exists again
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'scoring_config'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.True(t, colExists, "scoring_config column should exist after re-adding")
}

func TestPolicyTopologyMigrationUpDown(t *testing.T) {
	ctx := context.Background()

	pgServer := srv.NewDatabase(t)

	db, err := sql.Open("pgx", pgServer.ConnString())
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	bunDB := bun.NewDB(db, pgdialect.New())

	// Run all migrations
	err = Migrate(ctx, bunDB)
	require.NoError(t, err, "migration up should succeed")

	// Verify policy table exists
	var exists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
		)
	`).Scan(&exists)
	require.NoError(t, err)
	require.True(t, exists, "policy table should exist after migration up")

	// Verify topology column exists
	var colExists bool
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'topology'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.True(t, colExists, "topology column should exist")

	// Verify column type is varchar(10)
	var dataType string
	err = bunDB.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'policy'
		AND column_name = 'topology'
	`).Scan(&dataType)
	require.NoError(t, err)
	require.Equal(t, "character varying", dataType, "topology should be varchar type")

	// Verify column has character_maximum_length of 10
	var maxLength int
	err = bunDB.QueryRowContext(ctx, `
		SELECT character_maximum_length FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'policy'
		AND column_name = 'topology'
	`).Scan(&maxLength)
	require.NoError(t, err)
	require.Equal(t, 10, maxLength, "topology should have max length of 10")

	// Verify topology column has default value '1:1'
	var columnDefault sql.NullString
	err = bunDB.QueryRowContext(ctx, `
		SELECT column_default FROM information_schema.columns
		WHERE table_schema = 'reconciliations'
		AND table_name = 'policy'
		AND column_name = 'topology'
	`).Scan(&columnDefault)
	require.NoError(t, err)
	require.True(t, columnDefault.Valid, "topology column should have a default value")
	require.Contains(t, columnDefault.String, "1:1", "topology column default should be '1:1'")

	// Insert a policy without specifying topology to verify default value works
	_, err = bunDB.ExecContext(ctx, `
		INSERT INTO reconciliations.policy (id, created_at, name, ledger_name, ledger_query, payments_pool_id)
		VALUES ('423e4567-e89b-12d3-a456-426614174000', NOW(), 'Test Policy', 'test-ledger', '{}', '423e4567-e89b-12d3-a456-426614174001')
	`)
	require.NoError(t, err, "should be able to insert policy without specifying topology")

	// Verify the inserted policy has topology = '1:1'
	var topology string
	err = bunDB.QueryRowContext(ctx, `
		SELECT topology FROM reconciliations.policy WHERE id = '423e4567-e89b-12d3-a456-426614174000'
	`).Scan(&topology)
	require.NoError(t, err)
	require.Equal(t, "1:1", topology, "existing policies should have topology = '1:1'")

	// Verify we can insert a policy with topology = '1:N'
	_, err = bunDB.ExecContext(ctx, `
		INSERT INTO reconciliations.policy (id, created_at, name, ledger_name, ledger_query, payments_pool_id, topology)
		VALUES ('423e4567-e89b-12d3-a456-426614174002', NOW(), 'One to Many Policy', 'test-ledger', '{}', '423e4567-e89b-12d3-a456-426614174001', '1:N')
	`)
	require.NoError(t, err, "should be able to insert policy with topology = '1:N'")

	// Verify the 1:N policy has correct topology
	err = bunDB.QueryRowContext(ctx, `
		SELECT topology FROM reconciliations.policy WHERE id = '423e4567-e89b-12d3-a456-426614174002'
	`).Scan(&topology)
	require.NoError(t, err)
	require.Equal(t, "1:N", topology, "policy should have topology = '1:N'")

	// Verify we can insert a policy with topology = 'N:1'
	_, err = bunDB.ExecContext(ctx, `
		INSERT INTO reconciliations.policy (id, created_at, name, ledger_name, ledger_query, payments_pool_id, topology)
		VALUES ('423e4567-e89b-12d3-a456-426614174003', NOW(), 'Many to One Policy', 'test-ledger', '{}', '423e4567-e89b-12d3-a456-426614174001', 'N:1')
	`)
	require.NoError(t, err, "should be able to insert policy with topology = 'N:1'")

	// Verify the N:1 policy has correct topology
	err = bunDB.QueryRowContext(ctx, `
		SELECT topology FROM reconciliations.policy WHERE id = '423e4567-e89b-12d3-a456-426614174003'
	`).Scan(&topology)
	require.NoError(t, err)
	require.Equal(t, "N:1", topology, "policy should have topology = 'N:1'")

	// Test migration down
	err = DownPolicyTopology(ctx, bunDB)
	require.NoError(t, err, "migration down should succeed")

	// Verify topology column no longer exists
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'topology'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.False(t, colExists, "topology column should not exist after migration down")

	// Verify we can re-add the column manually (proving the down migration is reversible)
	_, err = bunDB.ExecContext(ctx, `
		ALTER TABLE reconciliations.policy ADD COLUMN topology VARCHAR(10) NOT NULL DEFAULT '1:1'
	`)
	require.NoError(t, err, "should be able to re-add topology column after down migration")

	// Verify column exists again
	err = bunDB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT FROM information_schema.columns
			WHERE table_schema = 'reconciliations'
			AND table_name = 'policy'
			AND column_name = 'topology'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	require.True(t, colExists, "topology column should exist after re-adding")
}
