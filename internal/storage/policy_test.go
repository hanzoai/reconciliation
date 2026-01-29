package storage

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	p1 = &models.Policy{
		ID:         uuid.New(),
		CreatedAt:  time.Now().Add(-1 * time.Hour),
		Name:       "test1",
		LedgerName: "default",
		LedgerQuery: map[string]interface{}{
			"$match": map[string]interface{}{
				"account": "test",
			},
		},
		PaymentsPoolID: uuid.New(),
	}

	p2 = &models.Policy{
		ID:         uuid.New(),
		CreatedAt:  time.Now().Add(-2 * time.Hour),
		Name:       "test2",
		LedgerName: "default",
		LedgerQuery: map[string]interface{}{
			"$match": map[string]interface{}{
				"account": "test",
			},
		},
		PaymentsPoolID: uuid.New(),
	}

	p3 = &models.Policy{
		ID:         uuid.New(),
		CreatedAt:  time.Now().Add(-3 * time.Hour),
		Name:       "test3",
		LedgerName: "default",
		LedgerQuery: map[string]interface{}{
			"$match": map[string]interface{}{
				"account": "test",
			},
		},
		PaymentsPoolID: uuid.New(),
	}
)

func insertPolicies(t *testing.T, store *Storage, policies ...*models.Policy) {
	for _, policy := range policies {
		err := store.CreatePolicy(context.Background(), policy)
		require.NoError(t, err)
	}
}

func TestMigratePolicyToTransactional(t *testing.T) {
	store := newStore(t)

	t.Run("successful migration", func(t *testing.T) {
		// Create a policy in balance mode
		policy := &models.Policy{
			ID:         uuid.New(),
			CreatedAt:  time.Now(),
			Name:       "test-migration-success",
			Mode:       "balance",
			Topology:   "1:1",
			LedgerName: "default",
			LedgerQuery: map[string]interface{}{
				"$match": map[string]interface{}{
					"account": "test",
				},
			},
			PaymentsPoolID: uuid.New(),
		}
		err := store.CreatePolicy(context.Background(), policy)
		require.NoError(t, err)

		// Migrate the policy
		scoringConfig := &models.ScoringConfig{
			TimeWindowHours:        24,
			AmountTolerancePercent: 0.0,
			MetadataFields:         []string{"order_id", "user_id"},
			Weights: &models.ScoringWeights{
				Amount:   0.4,
				Date:     0.3,
				Metadata: 0.3,
			},
			Thresholds: &models.ScoringThresholds{
				AutoMatch: 0.85,
				Review:    0.60,
			},
		}

		err = store.MigratePolicyToTransactional(
			context.Background(),
			policy.ID,
			[]string{"external_id"},
			"1:1",
			scoringConfig,
		)
		require.NoError(t, err)

		// Verify the migration
		updatedPolicy, err := store.GetPolicy(context.Background(), policy.ID)
		require.NoError(t, err)
		require.Equal(t, "transactional", updatedPolicy.Mode)
		require.Equal(t, []string{"external_id"}, updatedPolicy.DeterministicFields)
		require.Equal(t, "1:1", updatedPolicy.Topology)
		require.NotNil(t, updatedPolicy.ScoringConfig)
		require.Equal(t, 24, updatedPolicy.ScoringConfig.TimeWindowHours)
	})

	t.Run("policy already in transactional mode returns error", func(t *testing.T) {
		// Create a policy already in transactional mode
		policy := &models.Policy{
			ID:         uuid.New(),
			CreatedAt:  time.Now(),
			Name:       "test-already-transactional",
			Mode:       "transactional",
			Topology:   "1:1",
			LedgerName: "default",
			LedgerQuery: map[string]interface{}{
				"$match": map[string]interface{}{
					"account": "test",
				},
			},
			PaymentsPoolID: uuid.New(),
		}
		err := store.CreatePolicy(context.Background(), policy)
		require.NoError(t, err)

		// Try to migrate the policy (should fail)
		err = store.MigratePolicyToTransactional(
			context.Background(),
			policy.ID,
			[]string{"external_id"},
			"1:1",
			nil,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("non-existent policy returns error", func(t *testing.T) {
		// Try to migrate a non-existent policy
		err := store.MigratePolicyToTransactional(
			context.Background(),
			uuid.New(), // Random UUID that doesn't exist
			[]string{"external_id"},
			"1:1",
			nil,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestGetPolicyByLedgerName(t *testing.T) {
	store := newStore(t)

	t.Run("returns transactional policy for ledger name", func(t *testing.T) {
		// Create a transactional policy
		policy := &models.Policy{
			ID:         uuid.New(),
			CreatedAt:  time.Now(),
			Name:       "test-transactional",
			Mode:       "transactional",
			Topology:   "1:1",
			LedgerName: "test-ledger",
			LedgerQuery: map[string]interface{}{
				"$match": map[string]interface{}{
					"account": "test",
				},
			},
			PaymentsPoolID: uuid.New(),
		}
		err := store.CreatePolicy(context.Background(), policy)
		require.NoError(t, err)

		// Lookup by ledger name
		found, err := store.GetPolicyByLedgerName(context.Background(), "test-ledger")
		require.NoError(t, err)
		require.Equal(t, policy.ID, found.ID)
		require.Equal(t, "transactional", found.Mode)
	})

	t.Run("ignores balance mode policies", func(t *testing.T) {
		// Create a balance mode policy
		policy := &models.Policy{
			ID:         uuid.New(),
			CreatedAt:  time.Now(),
			Name:       "test-balance",
			Mode:       "balance",
			Topology:   "1:1",
			LedgerName: "balance-ledger",
			LedgerQuery: map[string]interface{}{
				"$match": map[string]interface{}{
					"account": "test",
				},
			},
			PaymentsPoolID: uuid.New(),
		}
		err := store.CreatePolicy(context.Background(), policy)
		require.NoError(t, err)

		// Lookup should not find balance mode policy
		_, err = store.GetPolicyByLedgerName(context.Background(), "balance-ledger")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns not found for unknown ledger name", func(t *testing.T) {
		_, err := store.GetPolicyByLedgerName(context.Background(), "unknown-ledger")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("returns most recent policy when multiple exist", func(t *testing.T) {
		// Create two transactional policies for the same ledger
		olderPolicy := &models.Policy{
			ID:         uuid.New(),
			CreatedAt:  time.Now().Add(-1 * time.Hour),
			Name:       "older-policy",
			Mode:       "transactional",
			Topology:   "1:1",
			LedgerName: "multi-policy-ledger",
			LedgerQuery: map[string]interface{}{
				"$match": map[string]interface{}{
					"account": "test",
				},
			},
			PaymentsPoolID: uuid.New(),
		}
		err := store.CreatePolicy(context.Background(), olderPolicy)
		require.NoError(t, err)

		newerPolicy := &models.Policy{
			ID:         uuid.New(),
			CreatedAt:  time.Now(),
			Name:       "newer-policy",
			Mode:       "transactional",
			Topology:   "1:1",
			LedgerName: "multi-policy-ledger",
			LedgerQuery: map[string]interface{}{
				"$match": map[string]interface{}{
					"account": "test",
				},
			},
			PaymentsPoolID: uuid.New(),
		}
		err = store.CreatePolicy(context.Background(), newerPolicy)
		require.NoError(t, err)

		// Should return the newer policy
		found, err := store.GetPolicyByLedgerName(context.Background(), "multi-policy-ledger")
		require.NoError(t, err)
		require.Equal(t, newerPolicy.ID, found.ID)
	})
}

func TestPolicyList(t *testing.T) {
	store := newStore(t)

	insertPolicies(t, store, p1, p2, p3)

	t.Run("nominal", func(t *testing.T) {
		policies, err := store.ListPolicies(context.Background(), GetPoliciesQuery{})
		require.NoError(t, err)
		require.Len(t, policies.Data, 3)
	})

	t.Run("with query id", func(t *testing.T) {
		policies, err := store.ListPolicies(context.Background(), GetPoliciesQuery{
			Options: PaginatedQueryOptions[PoliciesFilters]{
				QueryBuilder: query.Match("id", p1.ID.String()),
			},
		})

		require.NoError(t, err)
		require.Len(t, policies.Data, 1)
		require.Equal(t, policies.Data[0].ID, p1.ID)
	})

	t.Run("with query unknown id", func(t *testing.T) {
		policies, err := store.ListPolicies(context.Background(), GetPoliciesQuery{
			Options: PaginatedQueryOptions[PoliciesFilters]{
				QueryBuilder: query.Match("id", uuid.New().String()),
			},
		})
		require.NoError(t, err)
		require.Len(t, policies.Data, 0)
	})

	t.Run("with query name", func(t *testing.T) {
		policies, err := store.ListPolicies(context.Background(), GetPoliciesQuery{
			Options: PaginatedQueryOptions[PoliciesFilters]{
				QueryBuilder: query.Match("name", p1.Name),
			},
		})

		require.NoError(t, err)
		require.Len(t, policies.Data, 1)
		require.Equal(t, policies.Data[0].ID, p1.ID)
	})

	t.Run("with query unknown name", func(t *testing.T) {
		policies, err := store.ListPolicies(context.Background(), GetPoliciesQuery{
			Options: PaginatedQueryOptions[PoliciesFilters]{
				QueryBuilder: query.Match("name", "unknown"),
			},
		})

		require.NoError(t, err)
		require.Len(t, policies.Data, 0)
	})

	t.Run("with query createdAt", func(t *testing.T) {
		policies, err := store.ListPolicies(context.Background(), GetPoliciesQuery{
			Options: PaginatedQueryOptions[PoliciesFilters]{
				QueryBuilder: query.Lt("createdAt", p1.CreatedAt),
			},
		})

		require.NoError(t, err)
		require.Len(t, policies.Data, 2)
		require.Equal(t, policies.Data[0].ID, p2.ID)
		require.Equal(t, policies.Data[1].ID, p3.ID)
	})
}
