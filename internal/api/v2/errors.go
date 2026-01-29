package v2

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/reconciliation/internal/api/service"
	"github.com/formancehq/reconciliation/internal/storage"
)

const (
	ErrMissingOrInvalidBody = "MISSING_OR_INVALID_BODY"
	ErrValidation           = "VALIDATION"
	ErrInvalidID            = "INVALID_ID"
	ErrPendingReviewMatches = "PENDING_REVIEW_MATCHES"
)

func handleServiceErrors(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, service.ErrValidation):
		api.BadRequest(w, ErrValidation, err)
	case errors.Is(err, service.ErrInvalidID):
		api.BadRequest(w, ErrInvalidID, err)
	case errors.Is(err, storage.ErrInvalidQuery):
		api.BadRequest(w, ErrValidation, err)
	case errors.Is(err, storage.ErrNotFound):
		api.NotFound(w, err)
	case errors.Is(err, service.ErrPendingReviewMatches):
		api.WriteErrorResponse(w, http.StatusConflict, ErrPendingReviewMatches, err)
	default:
		api.InternalServerError(w, r, err)
	}
}
