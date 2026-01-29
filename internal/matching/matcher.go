package matching

import (
	"context"

	"github.com/formancehq/reconciliation/internal/models"
)

// Matcher defines the interface for matching transactions.
type Matcher interface {
	Match(ctx context.Context, transaction *models.Transaction) (*MatchResult, error)
}

// Decision represents the outcome of a matching operation.
type Decision string

const (
	DecisionMatched        Decision = "MATCHED"
	DecisionRequiresReview Decision = "REQUIRES_REVIEW"
	DecisionUnmatched      Decision = "UNMATCHED"
)

func (d Decision) String() string {
	return string(d)
}

func (d Decision) IsValid() bool {
	switch d {
	case DecisionMatched, DecisionRequiresReview, DecisionUnmatched:
		return true
	default:
		return false
	}
}

// Candidate represents a potential match for a transaction.
type Candidate struct {
	Transaction *models.Transaction `json:"transaction"`
	Score       float64             `json:"score"`
	Reasons     []string            `json:"reasons"`
}

// MatchResult represents the result of a matching operation.
type MatchResult struct {
	Match      *models.Match `json:"match,omitempty"`
	Candidates []Candidate   `json:"candidates"`
	Decision   Decision      `json:"decision"`
}
