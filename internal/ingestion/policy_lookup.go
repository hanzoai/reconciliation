package ingestion

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
)

// PolicyLookupService provides dynamic policy lookup by ledgerName or paymentsProvider.
type PolicyLookupService interface {
	// GetPolicyByLedgerName returns the transactional policy for the given ledgerName.
	// Returns storage.ErrNotFound if no matching policy exists.
	GetPolicyByLedgerName(ctx context.Context, ledgerName string) (*models.Policy, error)

	// GetPolicyByPaymentsProvider returns the transactional policy for the given paymentsProvider.
	// Returns storage.ErrNotFound if no matching policy exists.
	GetPolicyByPaymentsProvider(ctx context.Context, paymentsProvider string) (*models.Policy, error)

	// GetPolicyByConnectorType returns the transactional policy for the given connectorType (v2 API).
	// Returns storage.ErrNotFound if no matching policy exists.
	GetPolicyByConnectorType(ctx context.Context, connectorType string) (*models.Policy, error)

	// GetPolicyByConnectorID returns the transactional policy for the given connectorID (v2 API).
	// Returns storage.ErrNotFound if no matching policy exists.
	GetPolicyByConnectorID(ctx context.Context, connectorID string) (*models.Policy, error)
}

// cachedPolicy holds a cached policy with its expiration time.
type cachedPolicy struct {
	policy    *models.Policy
	err       error
	expiresAt time.Time
}

// CachingPolicyLookupService implements PolicyLookupService with caching.
// It caches policies by ledgerName with a configurable TTL to avoid
// repeated database queries while still allowing policies to be updated.
type CachingPolicyLookupService struct {
	store    *storage.Storage
	cache    map[string]*cachedPolicy
	cacheTTL time.Duration
	mu       sync.RWMutex
}

// CachingPolicyLookupConfig holds configuration for the caching policy lookup service.
type CachingPolicyLookupConfig struct {
	// CacheTTL is how long to cache policies before refreshing.
	// Defaults to 30 seconds if not specified.
	CacheTTL time.Duration
}

// DefaultPolicyLookupCacheTTL is the default cache TTL for the policy lookup service.
const DefaultPolicyLookupCacheTTL = 30 * time.Second

// NewCachingPolicyLookupService creates a new CachingPolicyLookupService.
func NewCachingPolicyLookupService(
	store *storage.Storage,
	config CachingPolicyLookupConfig,
) *CachingPolicyLookupService {
	ttl := config.CacheTTL
	if ttl <= 0 {
		ttl = DefaultPolicyLookupCacheTTL
	}

	return &CachingPolicyLookupService{
		store:    store,
		cache:    make(map[string]*cachedPolicy),
		cacheTTL: ttl,
	}
}

// GetPolicyByLedgerName returns the transactional policy for the given ledgerName.
// Results are cached for the configured TTL.
func (s *CachingPolicyLookupService) GetPolicyByLedgerName(ctx context.Context, ledgerName string) (*models.Policy, error) {
	cacheKey := "ledger:" + ledgerName
	return s.getOrRefresh(ctx, cacheKey, func() (*models.Policy, error) {
		return s.store.GetPolicyByLedgerName(ctx, ledgerName)
	})
}

// GetPolicyByPaymentsProvider returns the transactional policy for the given paymentsProvider.
// Results are cached for the configured TTL.
func (s *CachingPolicyLookupService) GetPolicyByPaymentsProvider(ctx context.Context, paymentsProvider string) (*models.Policy, error) {
	cacheKey := "payments:" + paymentsProvider
	return s.getOrRefresh(ctx, cacheKey, func() (*models.Policy, error) {
		return s.store.GetPolicyByPaymentsProvider(ctx, paymentsProvider)
	})
}

// GetPolicyByConnectorType returns the transactional policy for the given connectorType (v2 API).
// Results are cached for the configured TTL.
func (s *CachingPolicyLookupService) GetPolicyByConnectorType(ctx context.Context, connectorType string) (*models.Policy, error) {
	cacheKey := "connectorType:" + connectorType
	return s.getOrRefresh(ctx, cacheKey, func() (*models.Policy, error) {
		return s.store.GetPolicyByConnectorType(ctx, connectorType)
	})
}

// GetPolicyByConnectorID returns the transactional policy for the given connectorID (v2 API).
// Results are cached for the configured TTL.
func (s *CachingPolicyLookupService) GetPolicyByConnectorID(ctx context.Context, connectorID string) (*models.Policy, error) {
	cacheKey := "connectorID:" + connectorID
	return s.getOrRefresh(ctx, cacheKey, func() (*models.Policy, error) {
		return s.store.GetPolicyByConnectorID(ctx, connectorID)
	})
}

// getOrRefresh gets from cache or refreshes from the database using the provided fetch function.
func (s *CachingPolicyLookupService) getOrRefresh(ctx context.Context, cacheKey string, fetchFn func() (*models.Policy, error)) (*models.Policy, error) {
	// Try to get from cache first
	s.mu.RLock()
	cached, exists := s.cache[cacheKey]
	if exists && time.Now().Before(cached.expiresAt) {
		s.mu.RUnlock()
		return cached.policy, cached.err
	}
	s.mu.RUnlock()

	// Cache miss or expired, fetch from database
	return s.refreshPolicyWithKey(ctx, cacheKey, fetchFn)
}

// refreshPolicyWithKey fetches the policy from the database using the provided fetch function and updates the cache.
func (s *CachingPolicyLookupService) refreshPolicyWithKey(ctx context.Context, cacheKey string, fetchFn func() (*models.Policy, error)) (*models.Policy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check in case another goroutine refreshed while we were waiting
	cached, exists := s.cache[cacheKey]
	if exists && time.Now().Before(cached.expiresAt) {
		return cached.policy, cached.err
	}

	policy, err := fetchFn()

	// Log error if we get an unexpected error (but still cache it to avoid thundering herd)
	if err != nil && !isNotFoundError(err) {
		logging.FromContext(ctx).WithFields(map[string]interface{}{
			"cache_key": cacheKey,
			"error":     err.Error(),
		}).Error("failed to lookup policy")
	}

	// Cache the result (including errors) to avoid thundering herd
	s.cache[cacheKey] = &cachedPolicy{
		policy:    policy,
		err:       err,
		expiresAt: time.Now().Add(s.cacheTTL),
	}

	return policy, err
}

// isNotFoundError checks if the error is a not found error.
func isNotFoundError(err error) bool {
	return errors.Is(err, storage.ErrNotFound)
}

// Ensure CachingPolicyLookupService implements PolicyLookupService.
var _ PolicyLookupService = (*CachingPolicyLookupService)(nil)
