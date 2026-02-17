package migrations

import (
	"context"

	"github.com/formancehq/go-libs/migrations"
	"github.com/uptrace/bun"
)

func Migrate(ctx context.Context, db *bun.DB) error {
	migrator := migrations.NewMigrator()
	registerMigrations(migrator)

	return migrator.Up(ctx, db)
}

func registerMigrations(migrator *migrations.Migrator) {
	migrator.RegisterMigrations(
		migrations.Migration{
			Up: func(tx bun.Tx) error {
				_, err := tx.Exec(`
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
			Up: func(tx bun.Tx) error {
				_, err := tx.Exec(`
					ALTER TABLE reconciliations.reconciliation RENAME COLUMN reconciled_at TO reconciled_at_ledger;
					ALTER TABLE reconciliations.reconciliation ADD COLUMN reconciled_at_payments timestamp with time zone;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(tx bun.Tx) error {
				_, err := tx.Exec(`
					ALTER TABLE reconciliations.reconciliation ADD COLUMN drift_balances jsonb;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(tx bun.Tx) error {
				_, err := tx.Exec(`
					ALTER TABLE reconciliations.policy ADD COLUMN assertion_mode text NOT NULL DEFAULT 'COVERAGE';
					ALTER TABLE reconciliations.policy ADD COLUMN assertion_config jsonb NOT NULL DEFAULT '{}'::jsonb;
				`)
				return err
			},
		},
		migrations.Migration{
			Up: func(tx bun.Tx) error {
				_, err := tx.Exec(`
					ALTER TABLE reconciliations.policy ADD COLUMN policy_id uuid;
					UPDATE reconciliations.policy SET policy_id = id WHERE policy_id IS NULL;
					ALTER TABLE reconciliations.policy ALTER COLUMN policy_id SET NOT NULL;

					ALTER TABLE reconciliations.policy ADD COLUMN version bigint NOT NULL DEFAULT 1;
					ALTER TABLE reconciliations.policy ADD COLUMN lifecycle text NOT NULL DEFAULT 'ENABLED';

					CREATE UNIQUE INDEX IF NOT EXISTS reconciliations_policy_policy_id_version_ux
					ON reconciliations.policy (policy_id, version);
					CREATE INDEX IF NOT EXISTS reconciliations_policy_policy_id_version_idx
					ON reconciliations.policy (policy_id, version DESC);

					ALTER TABLE reconciliations.reconciliation DROP CONSTRAINT IF EXISTS reconciliation_policy_fk;
					ALTER TABLE reconciliations.reconciliation ADD COLUMN policy_version bigint NOT NULL DEFAULT 1;
				`)
				return err
			},
		},
	)
}
