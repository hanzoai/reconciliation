package ingestion

import (
	"context"
	"sync"

	"github.com/formancehq/reconciliation/internal/anomaly"
	"github.com/formancehq/reconciliation/internal/elasticsearch"
	"github.com/formancehq/reconciliation/internal/matching"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

// OrchestratorFactory creates and caches MatchingOrchestrators per policy.
// This allows multiple policies to be handled efficiently without creating
// redundant orchestrators.
type OrchestratorFactory struct {
	txStore     storage.TransactionStore
	matchRepo   storage.MatchRepository
	anomalyRepo storage.AnomalyRepository
	esClient    *elasticsearch.Client // optional, for probabilistic matching
	stack       string                // stack name for ES index pattern

	orchestrators map[uuid.UUID]*matching.MatchingOrchestrator
	mu            sync.RWMutex
}

// OrchestratorFactoryConfig holds configuration for the OrchestratorFactory.
type OrchestratorFactoryConfig struct {
	// NumWorkers is the number of concurrent matching workers per orchestrator.
	// This is currently not used at the orchestrator level, but kept for future use.
	NumWorkers int
}

// NewOrchestratorFactory creates a new OrchestratorFactory.
func NewOrchestratorFactory(
	txStore storage.TransactionStore,
	matchRepo storage.MatchRepository,
	anomalyRepo storage.AnomalyRepository,
	esClient *elasticsearch.Client,
	stack string,
) *OrchestratorFactory {
	return &OrchestratorFactory{
		txStore:       txStore,
		matchRepo:     matchRepo,
		anomalyRepo:   anomalyRepo,
		esClient:      esClient,
		stack:         stack,
		orchestrators: make(map[uuid.UUID]*matching.MatchingOrchestrator),
	}
}

// GetOrCreate returns an existing orchestrator for the policy or creates a new one.
// Orchestrators are cached by policy ID to avoid redundant creation.
func (f *OrchestratorFactory) GetOrCreate(policy *models.Policy) *matching.MatchingOrchestrator {
	f.mu.RLock()
	orchestrator, exists := f.orchestrators[policy.ID]
	f.mu.RUnlock()

	if exists {
		return orchestrator
	}

	// Create a new orchestrator
	return f.createOrchestrator(policy)
}

// createOrchestrator creates a new orchestrator for the policy.
func (f *OrchestratorFactory) createOrchestrator(policy *models.Policy) *matching.MatchingOrchestrator {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check in case another goroutine created it while we were waiting
	if orchestrator, exists := f.orchestrators[policy.ID]; exists {
		return orchestrator
	}

	// Create the anomaly resolver
	anomalyResolver := anomaly.NewDefaultResolver(f.anomalyRepo)

	// Create the anomaly detector
	anomalyDetector := &anomalyDetectorAdapter{
		detector: anomaly.NewDefaultDetector(),
	}

	// Create the orchestrator
	orchestrator := matching.NewMatchingOrchestrator(
		f.txStore,
		f.matchRepo,
		f.anomalyRepo,
		policy,
		nil, // No event emitter for now
		matching.OrchestratorConfig{
			ProbabilisticConfig: matching.DefaultProbabilisticMatcherConfig(),
		},
		f.esClient,
		f.stack,
		anomalyResolver,
		anomalyDetector,
	)

	// Cache it
	f.orchestrators[policy.ID] = orchestrator

	return orchestrator
}

// NumOrchestrators returns the number of cached orchestrators.
// This is primarily for testing and debugging.
func (f *OrchestratorFactory) NumOrchestrators() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.orchestrators)
}

// anomalyDetectorAdapter adapts anomaly.Detector to matching.AnomalyDetector interface.
type anomalyDetectorAdapter struct {
	detector anomaly.Detector
}

func (a *anomalyDetectorAdapter) Detect(ctx context.Context, transaction *models.Transaction, matchResult *matching.MatchResult, policyID uuid.UUID) (*matching.AnomalyDetectResult, error) {
	result, err := a.detector.Detect(ctx, transaction, matchResult, policyID)
	if err != nil {
		return nil, err
	}
	return &matching.AnomalyDetectResult{
		Anomaly: result.Anomaly,
		Pending: result.Pending,
	}, nil
}
