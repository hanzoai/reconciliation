package ingestion

import (
	"context"
	"sync"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

// PolicyProvider provides access to policy information with caching.
type PolicyProvider interface {
	// GetPolicy returns the policy, potentially from cache.
	GetPolicy(ctx context.Context) (*models.Policy, error)
	// RefreshPolicy forces a reload of the policy from storage.
	RefreshPolicy(ctx context.Context) (*models.Policy, error)
}

// CachingPolicyProvider implements PolicyProvider with a time-based cache.
// It caches the policy for a configurable duration to avoid repeated database queries,
// while still allowing the policy to be updated (e.g., mode changes) without restarting.
type CachingPolicyProvider struct {
	store    *storage.Storage
	policyID uuid.UUID
	cacheTTL time.Duration

	mu          sync.RWMutex
	cachedAt    time.Time
	cached      *models.Policy
	cacheErr    error
}

// CachingPolicyProviderConfig holds configuration for the caching policy provider.
type CachingPolicyProviderConfig struct {
	// CacheTTL is how long to cache the policy before refreshing.
	// Defaults to 30 seconds if not specified.
	CacheTTL time.Duration
}

// DefaultPolicyProviderCacheTTL is the default cache TTL for the policy provider.
const DefaultPolicyProviderCacheTTL = 30 * time.Second

// NewCachingPolicyProvider creates a new CachingPolicyProvider.
func NewCachingPolicyProvider(
	store *storage.Storage,
	policyID uuid.UUID,
	config CachingPolicyProviderConfig,
) *CachingPolicyProvider {
	ttl := config.CacheTTL
	if ttl <= 0 {
		ttl = DefaultPolicyProviderCacheTTL
	}

	return &CachingPolicyProvider{
		store:    store,
		policyID: policyID,
		cacheTTL: ttl,
	}
}

// GetPolicy returns the policy, using the cache if it's still valid.
func (p *CachingPolicyProvider) GetPolicy(ctx context.Context) (*models.Policy, error) {
	p.mu.RLock()
	if p.cached != nil && time.Since(p.cachedAt) < p.cacheTTL {
		policy := p.cached
		err := p.cacheErr
		p.mu.RUnlock()
		return policy, err
	}
	p.mu.RUnlock()

	// Cache miss or expired, refresh
	return p.RefreshPolicy(ctx)
}

// RefreshPolicy forces a reload of the policy from storage.
func (p *CachingPolicyProvider) RefreshPolicy(ctx context.Context) (*models.Policy, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check in case another goroutine refreshed while we were waiting
	if p.cached != nil && time.Since(p.cachedAt) < p.cacheTTL {
		return p.cached, p.cacheErr
	}

	policy, err := p.store.GetPolicy(ctx, p.policyID)
	p.cachedAt = time.Now()
	p.cached = policy
	p.cacheErr = err

	return policy, err
}

// Ensure CachingPolicyProvider implements PolicyProvider.
var _ PolicyProvider = (*CachingPolicyProvider)(nil)
