package ingestion

import (
	"testing"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestOrchestratorFactory_GetOrCreate(t *testing.T) {
	t.Run("creates new orchestrator for policy", func(t *testing.T) {
		factory := NewOrchestratorFactory(nil, nil, nil, nil, "test-stack")

		policy := &models.Policy{
			ID:         uuid.New(),
			Name:       "test-policy",
			Mode:       "transactional",
			LedgerName: "test-ledger",
		}

		orchestrator := factory.GetOrCreate(policy)
		require.NotNil(t, orchestrator)
		require.Equal(t, 1, factory.NumOrchestrators())
	})

	t.Run("returns cached orchestrator for same policy", func(t *testing.T) {
		factory := NewOrchestratorFactory(nil, nil, nil, nil, "test-stack")

		policy := &models.Policy{
			ID:         uuid.New(),
			Name:       "test-policy",
			Mode:       "transactional",
			LedgerName: "test-ledger",
		}

		orchestrator1 := factory.GetOrCreate(policy)
		orchestrator2 := factory.GetOrCreate(policy)

		require.Same(t, orchestrator1, orchestrator2)
		require.Equal(t, 1, factory.NumOrchestrators())
	})

	t.Run("creates separate orchestrators for different policies", func(t *testing.T) {
		factory := NewOrchestratorFactory(nil, nil, nil, nil, "test-stack")

		policy1 := &models.Policy{
			ID:         uuid.New(),
			Name:       "policy-1",
			Mode:       "transactional",
			LedgerName: "ledger-1",
		}

		policy2 := &models.Policy{
			ID:         uuid.New(),
			Name:       "policy-2",
			Mode:       "transactional",
			LedgerName: "ledger-2",
		}

		orchestrator1 := factory.GetOrCreate(policy1)
		orchestrator2 := factory.GetOrCreate(policy2)

		require.NotSame(t, orchestrator1, orchestrator2)
		require.Equal(t, 2, factory.NumOrchestrators())
	})
}
