package matching

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/reconciliation/internal/elasticsearch"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

// EventType represents the type of matching event.
type EventType string

const (
	EventTypeMatchCreated        EventType = "match.created"
	EventTypeMatchUnmatched      EventType = "match.unmatched"
	EventTypeMatchRequiresReview EventType = "match.requires_review"
)

// MatchEvent represents an event emitted by the orchestrator.
type MatchEvent struct {
	Type        EventType           `json:"type"`
	PolicyID    string              `json:"policyId"`
	Transaction *models.Transaction `json:"transaction"`
	Match       *models.Match       `json:"match,omitempty"`
	Decision    Decision            `json:"decision"`
	Candidates  []Candidate         `json:"candidates,omitempty"`
}

// EventEmitter defines the interface for emitting matching events.
type EventEmitter interface {
	Emit(ctx context.Context, event MatchEvent) error
}

// AnomalyResolver defines the interface for resolving anomalies when matches are created.
// This allows automatic closure of anomalies when late matches are found.
type AnomalyResolver interface {
	// ResolveForMatch checks if any transactions in the match have open anomalies
	// and resolves them automatically.
	ResolveForMatch(ctx context.Context, match *models.Match) error
}

// AnomalyDetectResult represents the result of anomaly detection.
type AnomalyDetectResult struct {
	Anomaly *models.Anomaly
	Pending bool // true if still within time window, anomaly not yet created
}

// AnomalyDetector defines the interface for detecting anomalies in transactions.
type AnomalyDetector interface {
	Detect(ctx context.Context, transaction *models.Transaction, matchResult *MatchResult, policyID uuid.UUID) (*AnomalyDetectResult, error)
}

// OrchestratorConfig holds configuration for the MatchingOrchestrator.
type OrchestratorConfig struct {
	// DeterministicConfig is the configuration for the DeterministicMatcher.
	DeterministicConfig DeterministicMatcherConfig
	// ProbabilisticConfig is the configuration for the ProbabilisticMatcher.
	ProbabilisticConfig ProbabilisticMatcherConfig
}

// MatchingOrchestrator orchestrates the matching pipeline.
// It coordinates deterministic matching, probabilistic matching,
// result persistence, and event emission.
type MatchingOrchestrator struct {
	txStore         storage.TransactionStore
	matchRepo       storage.MatchRepository
	anomalyRepo     storage.AnomalyRepository
	policy          *models.Policy
	emitter         EventEmitter
	config          OrchestratorConfig
	anomalyResolver AnomalyResolver
	anomalyDetector AnomalyDetector

	deterministicMatcher Matcher
	probabilisticMatcher Matcher
}

// NewMatchingOrchestrator creates a new MatchingOrchestrator instance.
func NewMatchingOrchestrator(
	txStore storage.TransactionStore,
	matchRepo storage.MatchRepository,
	anomalyRepo storage.AnomalyRepository,
	policy *models.Policy,
	emitter EventEmitter,
	config OrchestratorConfig,
	esClient *elasticsearch.Client,
	stack string,
	anomalyResolver AnomalyResolver,
	anomalyDetector AnomalyDetector,
) *MatchingOrchestrator {
	deterministicMatcher := NewDeterministicMatcher(txStore, policy, config.DeterministicConfig)

	// Create probabilistic matcher only if ES client is provided and policy is transactional with scoring
	var probabilisticMatcher Matcher
	if esClient != nil && policy.IsTransactional() {
		probabilisticMatcher = NewProbabilisticMatcher(esClient, policy, stack, config.ProbabilisticConfig)
	}

	return &MatchingOrchestrator{
		txStore:              txStore,
		matchRepo:            matchRepo,
		anomalyRepo:          anomalyRepo,
		policy:               policy,
		emitter:              emitter,
		config:               config,
		anomalyResolver:      anomalyResolver,
		anomalyDetector:      anomalyDetector,
		deterministicMatcher: deterministicMatcher,
		probabilisticMatcher: probabilisticMatcher,
	}
}

// NewMatchingOrchestratorWithMatchers creates a new MatchingOrchestrator with custom matchers.
// This is primarily used for testing to inject mock matchers.
func NewMatchingOrchestratorWithMatchers(
	matchRepo storage.MatchRepository,
	anomalyRepo storage.AnomalyRepository,
	policy *models.Policy,
	emitter EventEmitter,
	deterministicMatcher Matcher,
	probabilisticMatcher Matcher,
	anomalyResolver AnomalyResolver,
	anomalyDetector AnomalyDetector,
) *MatchingOrchestrator {
	return &MatchingOrchestrator{
		matchRepo:            matchRepo,
		anomalyRepo:          anomalyRepo,
		policy:               policy,
		emitter:              emitter,
		deterministicMatcher: deterministicMatcher,
		probabilisticMatcher: probabilisticMatcher,
		anomalyResolver:      anomalyResolver,
		anomalyDetector:      anomalyDetector,
	}
}

// Match processes a transaction through the matching pipeline.
// 1. Attempts deterministic matching first
// 2. If no match and policy is in transactional mode, calls ProbabilisticMatcher (Phase 3)
// 3. Persists the result via MatchRepository
// 4. Detects and persists anomalies for unmatched transactions
// 5. Emits match.created or match.unmatched event
func (o *MatchingOrchestrator) Match(ctx context.Context, transaction *models.Transaction) (*MatchResult, error) {
	if transaction == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	// Step 1: Try deterministic matching first
	result, err := o.deterministicMatcher.Match(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("deterministic matching failed: %w", err)
	}

	// Step 2: If no deterministic match and policy is transactional with scoring configured,
	// try probabilistic matching
	if result.Decision == DecisionUnmatched && o.policy.IsTransactional() && o.probabilisticMatcher != nil {
		result, err = o.probabilisticMatcher.Match(ctx, transaction)
		if err != nil {
			return nil, fmt.Errorf("probabilistic matching failed: %w", err)
		}
	}

	// Step 3: Persist the result if we have a match
	if result.Match != nil {
		txIDs := make([]uuid.UUID, 0, len(result.Match.LedgerTransactionIDs)+len(result.Match.PaymentsTransactionIDs))
		txIDs = append(txIDs, result.Match.LedgerTransactionIDs...)
		txIDs = append(txIDs, result.Match.PaymentsTransactionIDs...)

		if len(txIDs) > 0 {
			existing, err := o.matchRepo.FindByTransactionIDs(ctx, o.policy.ID, txIDs)
			if err != nil {
				return nil, fmt.Errorf("failed to check existing matches: %w", err)
			}
			if existing != nil {
				return nil, fmt.Errorf("transaction already part of match %s for policy %s", existing.ID, o.policy.ID)
			}
		}

		if err := o.matchRepo.Create(ctx, result.Match); err != nil {
			return nil, fmt.Errorf("failed to persist match: %w", err)
		}

		// Step 3b: Resolve any open anomalies for the matched transactions
		if o.anomalyResolver != nil {
			if err := o.anomalyResolver.ResolveForMatch(ctx, result.Match); err != nil {
				return nil, fmt.Errorf("failed to resolve anomalies for match: %w", err)
			}
		}
	}

	// Step 4: Detect and persist anomalies for unmatched transactions
	if o.anomalyDetector != nil && o.anomalyRepo != nil {
		detectResult, err := o.anomalyDetector.Detect(ctx, transaction, result, o.policy.ID)
		if err != nil {
			return nil, fmt.Errorf("anomaly detection failed: %w", err)
		}
		// If an anomaly was detected (not pending), persist it
		if detectResult != nil && detectResult.Anomaly != nil {
			if err := o.anomalyRepo.Create(ctx, detectResult.Anomaly); err != nil {
				return nil, fmt.Errorf("failed to persist anomaly: %w", err)
			}
		}
	}

	// Step 5: Emit the appropriate event
	if o.emitter != nil {
		event := o.buildEvent(transaction, result)
		if err := o.emitter.Emit(ctx, event); err != nil {
			return nil, fmt.Errorf("failed to emit event: %w", err)
		}
	}

	return result, nil
}

// buildEvent creates a MatchEvent based on the match result.
func (o *MatchingOrchestrator) buildEvent(transaction *models.Transaction, result *MatchResult) MatchEvent {
	eventType := EventTypeMatchUnmatched
	switch result.Decision {
	case DecisionMatched:
		eventType = EventTypeMatchCreated
	case DecisionRequiresReview:
		eventType = EventTypeMatchRequiresReview
	}

	return MatchEvent{
		Type:        eventType,
		PolicyID:    o.policy.ID.String(),
		Transaction: transaction,
		Match:       result.Match,
		Decision:    result.Decision,
		Candidates:  result.Candidates,
	}
}
