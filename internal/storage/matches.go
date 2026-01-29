package storage

import (
	"context"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

//go:generate mockgen -source matches.go -destination matches_mock.go -package storage . MatchRepository

type MatchRepository interface {
	Create(ctx context.Context, match *models.Match) error
	GetByID(ctx context.Context, id uuid.UUID) (*models.Match, error)
	ListByPolicy(ctx context.Context, q GetMatchesQuery) (*bunpaginate.Cursor[models.Match], error)
	UpdateDecision(ctx context.Context, id uuid.UUID, decision models.Decision) error
}

type MatchesFilters struct {
	PolicyID     *uuid.UUID
	Decision     *models.Decision
	ScoreMin     *float64
	CreatedAfter *time.Time
	CreatedBefore *time.Time
}

type GetMatchesQuery bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[MatchesFilters]]

func NewGetMatchesQuery(opts PaginatedQueryOptions[MatchesFilters]) GetMatchesQuery {
	return GetMatchesQuery{
		PageSize: opts.PageSize,
		Order:    bunpaginate.OrderAsc,
		Options:  opts,
	}
}

func (s *Storage) CreateMatch(ctx context.Context, match *models.Match) error {
	_, err := s.db.NewInsert().
		Model(match).
		Exec(ctx)
	if err != nil {
		return e("failed to create match", err)
	}

	return nil
}

func (s *Storage) GetMatchByID(ctx context.Context, id uuid.UUID) (*models.Match, error) {
	var match models.Match
	err := s.db.NewSelect().
		Model(&match).
		Where("id = ?", id).
		Scan(ctx)
	if err != nil {
		return nil, e("failed to get match", err)
	}

	return &match, nil
}

func (s *Storage) buildMatchListQuery(selectQuery *bun.SelectQuery, q GetMatchesQuery) *bun.SelectQuery {
	selectQuery = selectQuery.
		Order("created_at DESC")

	if q.Options.Options.PolicyID != nil {
		selectQuery = selectQuery.Where("policy_id = ?", *q.Options.Options.PolicyID)
	}

	if q.Options.Options.Decision != nil {
		selectQuery = selectQuery.Where("decision = ?", *q.Options.Options.Decision)
	}

	if q.Options.Options.ScoreMin != nil {
		selectQuery = selectQuery.Where("score >= ?", *q.Options.Options.ScoreMin)
	}

	if q.Options.Options.CreatedAfter != nil {
		selectQuery = selectQuery.Where("created_at > ?", *q.Options.Options.CreatedAfter)
	}

	if q.Options.Options.CreatedBefore != nil {
		selectQuery = selectQuery.Where("created_at < ?", *q.Options.Options.CreatedBefore)
	}

	return selectQuery
}

func (s *Storage) ListMatchesByPolicy(ctx context.Context, q GetMatchesQuery) (*bunpaginate.Cursor[models.Match], error) {
	return paginateWithOffset[PaginatedQueryOptions[MatchesFilters], models.Match](s, ctx,
		(*bunpaginate.OffsetPaginatedQuery[PaginatedQueryOptions[MatchesFilters]])(&q),
		func(query *bun.SelectQuery) *bun.SelectQuery {
			return s.buildMatchListQuery(query, q)
		},
	)
}

func (s *Storage) UpdateMatchDecision(ctx context.Context, id uuid.UUID, decision models.Decision) error {
	result, err := s.db.NewUpdate().
		Model((*models.Match)(nil)).
		Set("decision = ?", decision).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return e("failed to update match decision", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return e("failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return e("match not found", ErrNotFound)
	}

	return nil
}

// matchRepository wraps Storage to implement MatchRepository interface
type matchRepository struct {
	*Storage
}

// NewMatchRepository creates a new MatchRepository
func NewMatchRepository(s *Storage) MatchRepository {
	return &matchRepository{Storage: s}
}

func (r *matchRepository) Create(ctx context.Context, match *models.Match) error {
	return r.CreateMatch(ctx, match)
}

func (r *matchRepository) GetByID(ctx context.Context, id uuid.UUID) (*models.Match, error) {
	return r.GetMatchByID(ctx, id)
}

func (r *matchRepository) ListByPolicy(ctx context.Context, q GetMatchesQuery) (*bunpaginate.Cursor[models.Match], error) {
	return r.ListMatchesByPolicy(ctx, q)
}

func (r *matchRepository) UpdateDecision(ctx context.Context, id uuid.UUID, decision models.Decision) error {
	return r.UpdateMatchDecision(ctx, id, decision)
}

// DeleteMatchesInPeriod deletes all matches for a policy within a time period.
// Returns the number of matches deleted.
func (s *Storage) DeleteMatchesInPeriod(ctx context.Context, policyID uuid.UUID, from, to time.Time) (int64, error) {
	result, err := s.db.NewDelete().
		Model((*models.Match)(nil)).
		Where("policy_id = ?", policyID).
		Where("created_at >= ?", from).
		Where("created_at <= ?", to).
		Exec(ctx)
	if err != nil {
		return 0, e("failed to delete matches", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, e("failed to get rows affected", err)
	}

	return rowsAffected, nil
}

// FindMatchByTransactionIDs finds a match for a policy where any of the given transaction IDs
// appear in either ledger_tx_ids or payment_tx_ids arrays.
func (s *Storage) FindMatchByTransactionIDs(ctx context.Context, policyID uuid.UUID, txIDs []uuid.UUID) (*models.Match, error) {
	if len(txIDs) == 0 {
		return nil, nil
	}

	var match models.Match
	err := s.db.NewSelect().
		Model(&match).
		Where("policy_id = ?", policyID).
		Where("(ledger_tx_ids && ? OR payment_tx_ids && ?)", pgdialect.Array(txIDs), pgdialect.Array(txIDs)).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, e("failed to find match by transaction IDs", err)
	}

	return &match, nil
}

// CountMatchesByDecision counts the number of matches for a policy with a specific decision.
func (s *Storage) CountMatchesByDecision(ctx context.Context, policyID uuid.UUID, decision models.Decision) (int64, error) {
	count, err := s.db.NewSelect().
		Model((*models.Match)(nil)).
		Where("policy_id = ?", policyID).
		Where("decision = ?", decision).
		Count(ctx)
	if err != nil {
		return 0, e("failed to count matches", err)
	}

	return int64(count), nil
}
