package v1

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/reconciliation/internal/storage"
)

const (
	MaxPageSize     = 100
	DefaultPageSize = bunpaginate.QueryDefaultPageSize

	QueryKeyCursor   = "cursor"
	QueryKeyPageSize = "pageSize"
)

var (
	ErrInvalidPageSize = errors.New("invalid 'pageSize' query param")
)

func getPageSize(r *http.Request) (uint64, error) {
	pageSizeParam := r.URL.Query().Get(QueryKeyPageSize)
	if pageSizeParam == "" {
		return DefaultPageSize, nil
	}

	pageSize, err := strconv.ParseUint(pageSizeParam, 10, 32)
	if err != nil {
		return 0, ErrInvalidPageSize
	}

	if pageSize > MaxPageSize {
		return MaxPageSize, nil
	}

	return pageSize, nil
}

func getQueryBuilder(r *http.Request) (query.Builder, error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	if len(data) > 0 {
		return query.ParseJSON(string(data))
	}

	// If we don't have a body, we use the query param
	return query.ParseJSON(r.URL.Query().Get("query"))
}

func GetPaginatedQueryOptionsReconciliations(r *http.Request) (*storage.PaginatedQueryOptions[storage.ReconciliationsFilters], error) {
	qb, err := getQueryBuilder(r)
	if err != nil {
		return nil, err
	}

	pageSize, err := getPageSize(r)
	if err != nil {
		return nil, err
	}

	filters := storage.ReconciliationsFilters{}
	return pointer.For(storage.NewPaginatedQueryOptions(filters).
		WithQueryBuilder(qb).
		WithPageSize(pageSize)), nil
}

func GetPaginatedQueryOptionsPolicies(r *http.Request) (*storage.PaginatedQueryOptions[storage.PoliciesFilters], error) {
	qb, err := getQueryBuilder(r)
	if err != nil {
		return nil, err
	}

	pageSize, err := getPageSize(r)
	if err != nil {
		return nil, err
	}

	filters := storage.PoliciesFilters{}
	return pointer.For(storage.NewPaginatedQueryOptions(filters).
		WithQueryBuilder(qb).
		WithPageSize(pageSize)), nil
}
