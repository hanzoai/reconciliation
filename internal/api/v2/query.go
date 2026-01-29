package v2

import (
	"net/http"
	"strconv"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/pkg/errors"
)

const (
	MaxPageSize     = 100
	DefaultPageSize = bunpaginate.QueryDefaultPageSize

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

	var pageSize uint64
	var err error
	if pageSizeParam != "" {
		pageSize, err = strconv.ParseUint(pageSizeParam, 10, 32)
		if err != nil {
			return 0, ErrInvalidPageSize
		}
	}

	if pageSize > MaxPageSize {
		return MaxPageSize, nil
	}

	return pageSize, nil
}
