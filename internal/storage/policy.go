package storage

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

func (s *Storage) CreatePolicy(ctx context.Context, policy *models.Policy) error {
	_, err := s.db.NewInsert().
		Model(policy).
		Exec(ctx)
	if err != nil {
		return e("failed to create policy", err)
	}

	return nil
}

func (s *Storage) DeletePolicy(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.NewDelete().
		Model(&models.Policy{}).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return e("failed to delete policy", err)
	}

	return nil
}

func (s *Storage) UpdatePolicy(ctx context.Context, policy *models.Policy) error {
	result, err := s.db.NewUpdate().
		Model(policy).
		Column("name", "policy_mode", "topology", "deterministic_fields", "scoring_config", "payments_provider", "connector_type", "connector_id").
		Where("id = ?", policy.ID).
		Exec(ctx)
	if err != nil {
		return e("failed to update policy", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return e("failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return e("policy not found", ErrNotFound)
	}

	return nil
}

func (s *Storage) GetPolicy(ctx context.Context, id uuid.UUID) (*models.Policy, error) {
	var policy models.Policy
	err := s.db.NewSelect().
		Model(&policy).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get policy", err)
	}

	return &policy, nil
}

// GetPolicyByLedgerName returns the first transactional policy matching the ledgerName.
// Returns ErrNotFound if no matching policy exists.
// If multiple policies match, the first one ordered by created_at DESC is returned (with a warning).
func (s *Storage) GetPolicyByLedgerName(ctx context.Context, ledgerName string) (*models.Policy, error) {
	var policies []models.Policy
	err := s.db.NewSelect().
		Model(&policies).
		Where("ledger_name = ?", ledgerName).
		Where("policy_mode = ?", "transactional").
		Order("created_at DESC").
		Limit(2). // Get up to 2 to detect multiple policies
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get policy by ledger name", err)
	}

	if len(policies) == 0 {
		return nil, e("no transactional policy found for ledger", ErrNotFound)
	}

	return &policies[0], nil
}

// GetPolicyByPaymentsProvider returns the first transactional policy matching the paymentsProvider.
// Returns ErrNotFound if no matching policy exists.
// If multiple policies match, the first one ordered by created_at DESC is returned.
func (s *Storage) GetPolicyByPaymentsProvider(ctx context.Context, paymentsProvider string) (*models.Policy, error) {
	var policies []models.Policy
	err := s.db.NewSelect().
		Model(&policies).
		Where("payments_provider = ?", paymentsProvider).
		Where("policy_mode = ?", "transactional").
		Order("created_at DESC").
		Limit(2). // Get up to 2 to detect multiple policies
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get policy by payments provider", err)
	}

	if len(policies) == 0 {
		return nil, e("no transactional policy found for payments provider", ErrNotFound)
	}

	return &policies[0], nil
}

// GetPolicyByConnectorType returns the first transactional policy matching the connectorType.
// Returns ErrNotFound if no matching policy exists.
// If multiple policies match, the first one ordered by created_at DESC is returned.
func (s *Storage) GetPolicyByConnectorType(ctx context.Context, connectorType string) (*models.Policy, error) {
	var policies []models.Policy
	err := s.db.NewSelect().
		Model(&policies).
		Where("connector_type = ?", connectorType).
		Where("policy_mode = ?", "transactional").
		Order("created_at DESC").
		Limit(2). // Get up to 2 to detect multiple policies
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get policy by connector type", err)
	}

	if len(policies) == 0 {
		return nil, e("no transactional policy found for connector type", ErrNotFound)
	}

	return &policies[0], nil
}

// GetPolicyByConnectorID returns the first transactional policy matching the connectorID.
// Returns ErrNotFound if no matching policy exists.
// If multiple policies match, the first one ordered by created_at DESC is returned.
func (s *Storage) GetPolicyByConnectorID(ctx context.Context, connectorID string) (*models.Policy, error) {
	var policies []models.Policy
	err := s.db.NewSelect().
		Model(&policies).
		Where("connector_id = ?", connectorID).
		Where("policy_mode = ?", "transactional").
		Order("created_at DESC").
		Limit(2). // Get up to 2 to detect multiple policies
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get policy by connector ID", err)
	}

	if len(policies) == 0 {
		return nil, e("no transactional policy found for connector ID", ErrNotFound)
	}

	return &policies[0], nil
}

func (s *Storage) buildPolicyListQuery(selectQuery *bun.SelectQuery, q GetPoliciesQuery, where string, args []any) *bun.SelectQuery {
	selectQuery = selectQuery.
		Order("created_at DESC")

	if where != "" {
		return selectQuery.Where(where, args...)
	}

	return selectQuery
}

// TODO(polo): add pagination from go libs
func (s *Storage) ListPolicies(ctx context.Context, q GetPoliciesQuery) (*bunpaginate.Cursor[models.Policy], error) {
	var (
		where string
		args  []any
		err   error
	)

	if q.Options.QueryBuilder != nil {
		where, args, err = s.policyQueryContext(q.Options.QueryBuilder, q)
		if err != nil {
			return nil, err
		}
	}

	return paginateWithOffset[PaginatedQueryOptions[PoliciesFilters], models.Policy](s, ctx,
		(*bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[PoliciesFilters]])(&q),
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return s.buildPolicyListQuery(query, q, where, args)
		},
	)
}

func (s *Storage) policyQueryContext(qb query.Builder, q GetPoliciesQuery) (string, []any, error) {
	return qb.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
		switch key {
		case "id", "name":
			if operator != "$match" {
				return "", nil, errors.Wrap(ErrInvalidQuery, "'id' and 'name' columns can only be used with $match")
			}

			return fmt.Sprintf("%s = ?", key), []any{value}, nil
		case "createdAt":
			return fmt.Sprintf("created_at %s ?", query.DefaultComparisonOperatorsMapping[operator]), []any{value}, nil
		case "ledgerQuery":
			if operator != "$match" {
				return "", nil, errors.Wrap(ErrInvalidQuery, "'ledgerQuery' column can only be used with $match")
			}
			switch ledgerQuery := value.(type) {
			case string:
				return "ledger_query = ?", []any{ledgerQuery}, nil
			default:
				return "", nil, errors.Wrap(ErrInvalidQuery, "'ledgerQuery' column can only be used with string")
			}
		case "ledgerName":
			if operator != "$match" {
				return "", nil, errors.Wrap(ErrInvalidQuery, "'ledgerName' column can only be used with $match")
			}
			switch name := value.(type) {
			case string:
				return "ledger_name = ?", []any{name}, nil
			default:
				return "", nil, errors.Wrap(ErrInvalidQuery, "'ledgerName' column can only be used with string")
			}
		case "paymentsPoolID":
			if operator != "$match" {
				return "", nil, errors.Wrap(ErrInvalidQuery, "'paymentsPoolID' column can only be used with $match")
			}
			switch pID := value.(type) {
			case string:
				return "payments_pool_id = ?", []any{pID}, nil
			default:
				return "", nil, errors.Wrap(ErrInvalidQuery, "'paymentsPoolID' column can only be used with string")
			}
		default:
			return "", nil, errors.Wrapf(ErrInvalidQuery, "unknown key '%s' when building query", key)
		}
	}))
}

// MigratePolicyToTransactional updates a policy from balance mode to transactional mode
// and initializes the transactional-specific fields with the provided values.
func (s *Storage) MigratePolicyToTransactional(
	ctx context.Context,
	id uuid.UUID,
	deterministicFields []string,
	topology string,
	scoringConfig *models.ScoringConfig,
) error {
	result, err := s.db.NewUpdate().
		Model((*models.Policy)(nil)).
		Set("policy_mode = ?", "transactional").
		Set("deterministic_fields = ?", pgdialect.Array(deterministicFields)).
		Set("topology = ?", topology).
		Set("scoring_config = ?", scoringConfig).
		Where("id = ?", id).
		Where("policy_mode = ?", "balance").
		Exec(ctx)
	if err != nil {
		return e("failed to migrate policy to transactional", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return e("failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return e("policy not found or already in transactional mode", ErrNotFound)
	}

	return nil
}

type PoliciesFilters struct{}

type GetPoliciesQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[PoliciesFilters]]

func NewGetPoliciesQuery(opts PaginatedQueryOptions[PoliciesFilters]) GetPoliciesQuery {
	return GetPoliciesQuery{
		PageSize: opts.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  opts,
	}
}
