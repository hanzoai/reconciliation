package storage

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
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

func (s *Storage) GetPolicy(ctx context.Context, id uuid.UUID) (*models.Policy, error) {
	var policy models.Policy
	err := s.db.NewSelect().
		Model(&policy).
		Where("policy_id = ?", id).
		Order("version DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get policy", err)
	}

	return &policy, nil
}

func (s *Storage) GetPolicyVersion(ctx context.Context, policyID uuid.UUID, version int64) (*models.Policy, error) {
	var policy models.Policy
	err := s.db.NewSelect().
		Model(&policy).
		Where("policy_id = ?", policyID).
		Where("version = ?", version).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get policy", err)
	}

	return &policy, nil
}

func (s *Storage) CreatePolicyVersion(ctx context.Context, policy *models.Policy) error {
	return s.db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
		var latestVersion int64
		if err := tx.NewSelect().
			Model((*models.Policy)(nil)).
			ColumnExpr("COALESCE(MAX(version), 0)").
			Where("policy_id = ?", policy.PolicyID).
			Scan(ctx, &latestVersion); err != nil {
			return e("failed to get latest policy version", err)
		}

		policy.Version = latestVersion + 1
		_, err := tx.NewInsert().Model(policy).Exec(ctx)
		if err != nil {
			return e("failed to create policy version", err)
		}

		return nil
	})
}

func (s *Storage) ArchivePolicy(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.NewUpdate().
		Model((*models.Policy)(nil)).
		Set("lifecycle = ?", models.PolicyLifecycleArchived).
		Where("policy_id = ?", id).
		Exec(ctx)
	if err != nil {
		return e("failed to archive policy", err)
	}
	return nil
}

func (s *Storage) buildPolicyListQuery(selectQuery *bun.SelectQuery, q GetPoliciesQuery, where string, args []any) *bun.SelectQuery {
	latestPolicyRows := s.db.NewSelect().
		TableExpr("reconciliations.policy").
		ColumnExpr("DISTINCT ON (policy_id) id").
		OrderExpr("policy_id, version DESC")

	selectQuery = selectQuery.
		Where("id IN (?)", latestPolicyRows).
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
			if key == "id" {
				return "policy_id = ?", []any{value}, nil
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

type PoliciesFilters struct{}

type GetPoliciesQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[PoliciesFilters]]

func NewGetPoliciesQuery(opts PaginatedQueryOptions[PoliciesFilters]) GetPoliciesQuery {
	return GetPoliciesQuery{
		PageSize: opts.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  opts,
	}
}
