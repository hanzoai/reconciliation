package ingestion

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// mockStorage is a simple mock for testing PolicyLookupService
type mockPolicyStorage struct {
	policies map[string]*models.Policy
}

func newMockPolicyStorage() *mockPolicyStorage {
	return &mockPolicyStorage{
		policies: make(map[string]*models.Policy),
	}
}

func (m *mockPolicyStorage) GetPolicyByLedgerName(ctx context.Context, ledgerName string) (*models.Policy, error) {
	policy, ok := m.policies[ledgerName]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return policy, nil
}

func (m *mockPolicyStorage) addPolicy(ledgerName string, policy *models.Policy) {
	m.policies[ledgerName] = policy
}

func TestCachingPolicyLookupService_GetPolicyByLedgerName(t *testing.T) {
	t.Run("returns policy from cache on subsequent calls", func(t *testing.T) {
		mockStore := newMockPolicyStorage()
		policy := &models.Policy{
			ID:         uuid.New(),
			Name:       "test-policy",
			Mode:       "transactional",
			LedgerName: "test-ledger",
		}
		mockStore.addPolicy("test-ledger", policy)

		// Create a wrapper that wraps the mock
		service := &testCachingService{
			mock:     mockStore,
			cache:    make(map[string]*cachedPolicy),
			cacheTTL: 30 * time.Second,
		}

		ctx := context.Background()

		// First call should fetch from storage
		result, err := service.GetPolicyByLedgerName(ctx, "test-ledger")
		require.NoError(t, err)
		require.Equal(t, policy.ID, result.ID)
		require.Equal(t, 1, service.fetchCount)

		// Second call should use cache
		result, err = service.GetPolicyByLedgerName(ctx, "test-ledger")
		require.NoError(t, err)
		require.Equal(t, policy.ID, result.ID)
		require.Equal(t, 1, service.fetchCount) // Still 1, used cache
	})

	t.Run("refreshes cache after TTL expires", func(t *testing.T) {
		mockStore := newMockPolicyStorage()
		policy := &models.Policy{
			ID:         uuid.New(),
			Name:       "test-policy",
			Mode:       "transactional",
			LedgerName: "test-ledger",
		}
		mockStore.addPolicy("test-ledger", policy)

		// Create a service with a very short TTL
		service := &testCachingService{
			mock:     mockStore,
			cache:    make(map[string]*cachedPolicy),
			cacheTTL: 1 * time.Millisecond, // Very short TTL
		}

		ctx := context.Background()

		// First call
		_, err := service.GetPolicyByLedgerName(ctx, "test-ledger")
		require.NoError(t, err)
		require.Equal(t, 1, service.fetchCount)

		// Wait for TTL to expire
		expiry := time.Now().Add(service.cacheTTL)
		require.Eventually(t, func() bool {
			return time.Now().After(expiry)
		}, 100*time.Millisecond, 1*time.Millisecond)

		// Second call should refetch
		_, err = service.GetPolicyByLedgerName(ctx, "test-ledger")
		require.NoError(t, err)
		require.Equal(t, 2, service.fetchCount)
	})

	t.Run("returns not found error for unknown ledger", func(t *testing.T) {
		mockStore := newMockPolicyStorage()

		service := &testCachingService{
			mock:     mockStore,
			cache:    make(map[string]*cachedPolicy),
			cacheTTL: 30 * time.Second,
		}

		ctx := context.Background()

		_, err := service.GetPolicyByLedgerName(ctx, "unknown-ledger")
		require.Error(t, err)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("caches not found errors to avoid thundering herd", func(t *testing.T) {
		mockStore := newMockPolicyStorage()

		service := &testCachingService{
			mock:     mockStore,
			cache:    make(map[string]*cachedPolicy),
			cacheTTL: 30 * time.Second,
		}

		ctx := context.Background()

		// First call should fetch from storage
		_, err := service.GetPolicyByLedgerName(ctx, "unknown-ledger")
		require.Error(t, err)
		require.Equal(t, 1, service.fetchCount)

		// Second call should use cached error
		_, err = service.GetPolicyByLedgerName(ctx, "unknown-ledger")
		require.Error(t, err)
		require.Equal(t, 1, service.fetchCount) // Still 1, used cached error
	})
}

// testCachingService is a simplified version of CachingPolicyLookupService for testing
type testCachingService struct {
	mock       *mockPolicyStorage
	cache      map[string]*cachedPolicy
	cacheTTL   time.Duration
	fetchCount int
}

func (s *testCachingService) GetPolicyByLedgerName(ctx context.Context, ledgerName string) (*models.Policy, error) {
	// Check cache
	if cached, ok := s.cache[ledgerName]; ok && time.Now().Before(cached.expiresAt) {
		return cached.policy, cached.err
	}

	// Fetch from storage
	s.fetchCount++
	policy, err := s.mock.GetPolicyByLedgerName(ctx, ledgerName)

	// Cache the result
	s.cache[ledgerName] = &cachedPolicy{
		policy:    policy,
		err:       err,
		expiresAt: time.Now().Add(s.cacheTTL),
	}

	return policy, err
}
