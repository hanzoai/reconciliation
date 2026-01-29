package migrations

import (
	"context"

	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/uptrace/bun"
)

func Migrate(ctx context.Context, db *bun.DB) error {
	migrator := migrations.NewMigrator(db)
	registerMigrations(migrator)

	return migrator.Up(ctx)
}

// DownTransaction drops the transaction table (migration #4 rollback)
// This is used for testing purposes to verify the migration is reversible
// Note: Also drops anomaly table first since it has a FK reference to transaction
func DownTransaction(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		DROP TABLE IF EXISTS reconciliations.anomaly;
		DROP TABLE IF EXISTS reconciliations.transaction;
	`)
	return err
}

// DownMatch drops the match table (migration #5 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownMatch(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		DROP TABLE IF EXISTS reconciliations.match;
	`)
	return err
}

// DownAnomaly drops the anomaly table (migration #6 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownAnomaly(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		DROP TABLE IF EXISTS reconciliations.anomaly;
	`)
	return err
}

// DownPolicyMode drops the policy_mode column from policy table (migration #7 + #13 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownPolicyMode(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		ALTER TABLE reconciliations.policy DROP COLUMN IF EXISTS policy_mode;
	`)
	return err
}

// DownPolicyDeterministicFields drops the deterministic_fields column from policy table (migration #8 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownPolicyDeterministicFields(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		ALTER TABLE reconciliations.policy DROP COLUMN IF EXISTS deterministic_fields;
	`)
	return err
}

// DownPolicyScoringConfig drops the scoring_config column from policy table (migration #9 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownPolicyScoringConfig(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		ALTER TABLE reconciliations.policy DROP COLUMN IF EXISTS scoring_config;
	`)
	return err
}

// DownPolicyTopology drops the topology column from policy table (migration #10 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownPolicyTopology(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		ALTER TABLE reconciliations.policy DROP COLUMN IF EXISTS topology;
	`)
	return err
}

// DownAnomalyResolvedBy drops the resolved_by column from anomaly table (migration #11 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownAnomalyResolvedBy(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		ALTER TABLE reconciliations.anomaly DROP COLUMN IF EXISTS resolved_by;
	`)
	return err
}

// DownReport drops the report table (migration #12 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownReport(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		DROP TABLE IF EXISTS reconciliations.report;
	`)
	return err
}

// DownConnectorColumns drops the connector_type and connector_id columns (migration #17 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownConnectorColumns(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		ALTER TABLE reconciliations.policy DROP CONSTRAINT IF EXISTS policy_connector_exclusive;
		DROP INDEX IF EXISTS reconciliations.policy_connector_type_idx;
		DROP INDEX IF EXISTS reconciliations.policy_connector_id_idx;
		ALTER TABLE reconciliations.policy DROP COLUMN IF EXISTS connector_type;
		ALTER TABLE reconciliations.policy DROP COLUMN IF EXISTS connector_id;
	`)
	return err
}

// DownTransactionPolicyNullable reverts policy_id to NOT NULL (migration #18 rollback)
// This is used for testing purposes to verify the migration is reversible
func DownTransactionPolicyNullable(ctx context.Context, db *bun.DB) error {
	_, err := db.ExecContext(ctx, `
		-- Drop the new unique constraint
		ALTER TABLE reconciliations.transaction DROP CONSTRAINT IF EXISTS transaction_side_external_id_unique;

		-- Drop the policy_id index
		DROP INDEX IF EXISTS reconciliations.transaction_policy_id_idx;

		-- Make policy_id NOT NULL again (will fail if there are null values)
		ALTER TABLE reconciliations.transaction ALTER COLUMN policy_id SET NOT NULL;

		-- Recreate the original unique constraint
		ALTER TABLE reconciliations.transaction ADD CONSTRAINT transaction_policy_side_external_id_unique
			UNIQUE (policy_id, side, external_id);

		-- Recreate the foreign key constraint
		ALTER TABLE reconciliations.transaction ADD CONSTRAINT transaction_policy_fk
			FOREIGN KEY (policy_id)
			REFERENCES reconciliations.policy (id)
			ON DELETE CASCADE
			NOT DEFERRABLE
			INITIALLY IMMEDIATE;
	`)
	return err
}

// DownDropTransactionTable is a no-op (migration #19 rollback)
// The transaction table was dropped and should not be recreated.
// Transactions are now stored exclusively in OpenSearch.
func DownDropTransactionTable(ctx context.Context, db *bun.DB) error {
	// No rollback - transactions are now stored in OpenSearch
	return nil
}

func registerMigrations(migrator *migrations.Migrator) {
	migrator.RegisterMigrations(
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					CREATE SCHEMA IF NOT EXISTS reconciliations;

					CREATE TABLE IF NOT EXISTS reconciliations.policy (
						id uuid NOT NULL,
						created_at timestamp with time zone NOT NULL,
						name text NOT NULL,
						ledger_name text NOT NULL,
						ledger_query jsonb NOT NULL,
						payments_pool_id uuid NOT NULL,
						CONSTRAINT policy_pk PRIMARY KEY (id)
					);

					CREATE TABLE IF NOT EXISTS reconciliations.reconciliation (
						id uuid NOT NULL,
						policy_id uuid NOT NULL,
						created_at timestamp with time zone NOT NULL UNIQUE,
						reconciled_at timestamp with time zone,
						status text NOT NULL,
						ledger_balances jsonb NOT NULL,
						payments_balances jsonb NOT NULL,
						error text,
					   	CONSTRAINT reconciliation_pk PRIMARY KEY (id)
					);

					ALTER TABLE reconciliations.reconciliation DROP CONSTRAINT IF EXISTS reconciliation_policy_fk;
					ALTER TABLE reconciliations.reconciliation ADD CONSTRAINT reconciliation_policy_fk
					FOREIGN KEY (policy_id)
					REFERENCES reconciliations.policy (id)
					ON DELETE CASCADE
					NOT DEFERRABLE
					INITIALLY IMMEDIATE
					;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.reconciliation RENAME COLUMN reconciled_at TO reconciled_at_ledger;
					ALTER TABLE reconciliations.reconciliation ADD COLUMN reconciled_at_payments timestamp with time zone;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.reconciliation ADD COLUMN drift_balances jsonb;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					CREATE TABLE IF NOT EXISTS reconciliations.transaction (
						id uuid NOT NULL,
						policy_id uuid NOT NULL,
						side varchar(10) NOT NULL,
						provider varchar(100) NOT NULL,
						external_id varchar(255) NOT NULL,
						amount bigint NOT NULL,
						currency varchar(3) NOT NULL,
						occurred_at timestamp with time zone NOT NULL,
						ingested_at timestamp with time zone NOT NULL,
						metadata jsonb,
						CONSTRAINT transaction_pk PRIMARY KEY (id),
						CONSTRAINT transaction_policy_side_external_id_unique UNIQUE (policy_id, side, external_id)
					);

					CREATE INDEX IF NOT EXISTS transaction_policy_id_side_idx ON reconciliations.transaction (policy_id, side);
					CREATE INDEX IF NOT EXISTS transaction_external_id_idx ON reconciliations.transaction (external_id);

					ALTER TABLE reconciliations.transaction DROP CONSTRAINT IF EXISTS transaction_policy_fk;
					ALTER TABLE reconciliations.transaction ADD CONSTRAINT transaction_policy_fk
						FOREIGN KEY (policy_id)
						REFERENCES reconciliations.policy (id)
						ON DELETE CASCADE
						NOT DEFERRABLE
						INITIALLY IMMEDIATE;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
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
					);

					CREATE INDEX IF NOT EXISTS match_policy_id_decision_idx ON reconciliations.match (policy_id, decision);

					ALTER TABLE reconciliations.match DROP CONSTRAINT IF EXISTS match_policy_fk;
					ALTER TABLE reconciliations.match ADD CONSTRAINT match_policy_fk
						FOREIGN KEY (policy_id)
						REFERENCES reconciliations.policy (id)
						ON DELETE CASCADE
						NOT DEFERRABLE
						INITIALLY IMMEDIATE;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
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
					);

					CREATE INDEX IF NOT EXISTS anomaly_policy_id_state_idx ON reconciliations.anomaly (policy_id, state);

					ALTER TABLE reconciliations.anomaly DROP CONSTRAINT IF EXISTS anomaly_policy_fk;
					ALTER TABLE reconciliations.anomaly ADD CONSTRAINT anomaly_policy_fk
						FOREIGN KEY (policy_id)
						REFERENCES reconciliations.policy (id)
						ON DELETE CASCADE
						NOT DEFERRABLE
						INITIALLY IMMEDIATE;

					ALTER TABLE reconciliations.anomaly DROP CONSTRAINT IF EXISTS anomaly_transaction_fk;
					ALTER TABLE reconciliations.anomaly ADD CONSTRAINT anomaly_transaction_fk
						FOREIGN KEY (transaction_id)
						REFERENCES reconciliations.transaction (id)
						ON DELETE CASCADE
						NOT DEFERRABLE
						INITIALLY IMMEDIATE;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.policy ADD COLUMN mode varchar(20) NOT NULL DEFAULT 'balance';
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.policy ADD COLUMN deterministic_fields TEXT[];
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.policy ADD COLUMN scoring_config JSONB;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.policy ADD COLUMN topology VARCHAR(10) NOT NULL DEFAULT '1:1';
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.anomaly ADD COLUMN resolved_by VARCHAR(255);
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					CREATE TABLE IF NOT EXISTS reconciliations.report (
						id uuid NOT NULL,
						policy_id uuid NOT NULL,
						period_start timestamp with time zone NOT NULL,
						period_end timestamp with time zone NOT NULL,
						total_transactions bigint NOT NULL,
						matched_count bigint NOT NULL,
						match_rate decimal(10,6) NOT NULL,
						anomalies_by_type jsonb NOT NULL DEFAULT '{}',
						generated_at timestamp with time zone NOT NULL,
						CONSTRAINT report_pk PRIMARY KEY (id)
					);

					CREATE INDEX IF NOT EXISTS report_policy_id_generated_at_idx ON reconciliations.report (policy_id, generated_at DESC);

					ALTER TABLE reconciliations.report DROP CONSTRAINT IF EXISTS report_policy_fk;
					ALTER TABLE reconciliations.report ADD CONSTRAINT report_policy_fk
						FOREIGN KEY (policy_id)
						REFERENCES reconciliations.policy (id)
						ON DELETE CASCADE
						NOT DEFERRABLE
						INITIALLY IMMEDIATE;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.policy RENAME COLUMN mode TO policy_mode;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					CREATE TABLE IF NOT EXISTS reconciliations.matcher (
						id uuid NOT NULL,
						policy_id uuid NOT NULL,
						name text NOT NULL,
						deterministic_fields text[],
						scoring_config jsonb,
						created_at timestamp with time zone NOT NULL,
						CONSTRAINT matcher_pk PRIMARY KEY (id)
					);

					CREATE INDEX IF NOT EXISTS matcher_policy_id_idx ON reconciliations.matcher (policy_id);

					ALTER TABLE reconciliations.matcher DROP CONSTRAINT IF EXISTS matcher_policy_fk;
					ALTER TABLE reconciliations.matcher ADD CONSTRAINT matcher_policy_fk
						FOREIGN KEY (policy_id)
						REFERENCES reconciliations.policy (id)
						ON DELETE CASCADE
						NOT DEFERRABLE
						INITIALLY IMMEDIATE;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				// Expand currency field to support Formance asset format (e.g., USD/2, EUR/2, custom assets)
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.transaction ALTER COLUMN currency TYPE varchar(50);
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				// Add payments_provider field for dynamic payments lookup (like ledger_name for ledger)
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.policy ADD COLUMN payments_provider VARCHAR(100);
					CREATE INDEX IF NOT EXISTS policy_payments_provider_idx ON reconciliations.policy (payments_provider) WHERE payments_provider IS NOT NULL;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				// Add connector_type and connector_id columns for v2 API
				// These are mutually exclusive - only one can be set per policy
				_, err := db.ExecContext(ctx, `
					ALTER TABLE reconciliations.policy ADD COLUMN connector_type VARCHAR(100);
					ALTER TABLE reconciliations.policy ADD COLUMN connector_id VARCHAR(255);

					-- Partial indexes for efficient lookup
					CREATE INDEX IF NOT EXISTS policy_connector_type_idx ON reconciliations.policy (connector_type) WHERE connector_type IS NOT NULL;
					CREATE INDEX IF NOT EXISTS policy_connector_id_idx ON reconciliations.policy (connector_id) WHERE connector_id IS NOT NULL;

					-- Constraint: connector_type and connector_id are mutually exclusive
					ALTER TABLE reconciliations.policy ADD CONSTRAINT policy_connector_exclusive
						CHECK (NOT (connector_type IS NOT NULL AND connector_id IS NOT NULL));
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				// Make policy_id nullable in transaction table for decoupled ingestion
				// Transactions can now be ingested without being associated to a policy
				_, err := db.ExecContext(ctx, `
					-- Drop the foreign key constraint (transactions can exist without policy)
					ALTER TABLE reconciliations.transaction DROP CONSTRAINT IF EXISTS transaction_policy_fk;

					-- Drop the old unique constraint
					ALTER TABLE reconciliations.transaction DROP CONSTRAINT IF EXISTS transaction_policy_side_external_id_unique;

					-- Make policy_id nullable
					ALTER TABLE reconciliations.transaction ALTER COLUMN policy_id DROP NOT NULL;

					-- Create new unique constraint: (side, external_id) must be unique
					-- This ensures we don't have duplicate transactions from the same source
					ALTER TABLE reconciliations.transaction ADD CONSTRAINT transaction_side_external_id_unique
						UNIQUE (side, external_id);

					-- Keep an index on policy_id for queries that filter by policy
					CREATE INDEX IF NOT EXISTS transaction_policy_id_idx ON reconciliations.transaction (policy_id) WHERE policy_id IS NOT NULL;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				// Migration #19: Drop transaction table - transactions are now stored in OpenSearch
				//
				// This is a breaking change that removes the PostgreSQL transactions table.
				// All transaction data is now stored exclusively in OpenSearch.
				//
				// Key changes:
				// - Transaction storage moved to OpenSearch for better scalability
				// - Metadata field searches now use OpenSearch's flat_object type
				// - Deduplication uses OpenSearch's idempotent document IDs
				//
				// Note: The anomaly table's FK to transaction is dropped first.
				// Anomalies now store transaction_id as a reference to OpenSearch documents.
				_, err := db.ExecContext(ctx, `
					-- Drop foreign key constraint from anomaly table
					ALTER TABLE reconciliations.anomaly DROP CONSTRAINT IF EXISTS anomaly_transaction_fk;

					-- Drop the transaction table
					DROP TABLE IF EXISTS reconciliations.transaction;
				`)
				return err
			},
		},
		// Migration #20: Drop matcher table
		// The matcher table was unused - matching uses Policy configuration directly.
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					DROP TABLE IF EXISTS reconciliations.matcher;
				`)
				return err
			},
		},
		// Migration #21: Create backfill table for historical data ingestion
		migrations.Migration{
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					CREATE TABLE IF NOT EXISTS reconciliations.backfill (
						id uuid NOT NULL,
						source varchar(20) NOT NULL,
						ledger varchar(255),
						since timestamp with time zone NOT NULL,
						status varchar(20) NOT NULL DEFAULT 'pending',
						ingested bigint NOT NULL DEFAULT 0,
						last_cursor text,
						error_message text,
						created_at timestamp with time zone NOT NULL,
						updated_at timestamp with time zone NOT NULL,
						CONSTRAINT backfill_pk PRIMARY KEY (id)
					);

					CREATE INDEX IF NOT EXISTS backfill_status_idx ON reconciliations.backfill (status);
					CREATE INDEX IF NOT EXISTS backfill_created_at_idx ON reconciliations.backfill (created_at DESC);
				`)
				return err
			},
		},
	)
}
