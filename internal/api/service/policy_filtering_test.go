package service

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPolicyFiltering_AnomaliesNotSharedBetweenPolicies verifies that anomalies
// created for one policy are NOT returned when querying another policy's anomalies.
// This is a critical business requirement: each policy should have its own isolated set of anomalies.
func TestPolicyFiltering_AnomaliesNotSharedBetweenPolicies(t *testing.T) {
	ctx := context.Background()
	store := newMockStoreForPolicyFilteringTest()

	// Create two policies with DIFFERENT providers
	policy1ID := uuid.New()
	connectorType1 := "provider-1"
	policy1 := &models.Policy{
		ID:            policy1ID,
		Name:          "test-policy-1",
		Mode:          "transactional",
		LedgerName:    "ledger-1",
		ConnectorType: &connectorType1,
	}
	store.policies[policy1ID] = policy1

	policy2ID := uuid.New()
	connectorType2 := "provider-2"
	policy2 := &models.Policy{
		ID:            policy2ID,
		Name:          "test-policy-2",
		Mode:          "transactional",
		LedgerName:    "ledger-2",
		ConnectorType: &connectorType2,
	}
	store.policies[policy2ID] = policy2

	// Create a transaction for policy1 (old enough to trigger anomaly)
	tx1ID := uuid.New()
	tx1 := &models.Transaction{
		ID:         tx1ID,
		PolicyID:   nil, // No policy association (simplified ingestion)
		Side:       models.TransactionSideLedger,
		Provider:   "ledger-1",
		ExternalID: "tx-for-policy-1",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now().UTC().Add(-25 * time.Hour),
		IngestedAt: time.Now().UTC(),
	}
	store.transactions[tx1ID] = tx1
	store.transactionsByProvider["ledger-1"] = append(store.transactionsByProvider["ledger-1"], tx1)

	// Create a transaction for policy2 (old enough to trigger anomaly)
	tx2ID := uuid.New()
	tx2 := &models.Transaction{
		ID:         tx2ID,
		PolicyID:   nil,
		Side:       models.TransactionSideLedger,
		Provider:   "ledger-2",
		ExternalID: "tx-for-policy-2",
		Amount:     2000,
		Currency:   "EUR",
		OccurredAt: time.Now().UTC().Add(-25 * time.Hour),
		IngestedAt: time.Now().UTC(),
	}
	store.transactions[tx2ID] = tx2
	store.transactionsByProvider["ledger-2"] = append(store.transactionsByProvider["ledger-2"], tx2)

	txStore := &mockTxStoreForPolicyFilteringTest{transactions: store.transactions}
	svc := NewService(store, txStore, nil)

	// Run matching for policy1 - should create anomaly for tx1 only
	results1, err := svc.MatchTransactions(ctx, policy1ID.String(), []string{tx1ID.String()})
	require.NoError(t, err)
	require.Len(t, results1, 1)
	assert.Equal(t, "UNMATCHED", results1[0].Decision)

	// Verify anomaly was created for policy1
	anomaliesForPolicy1 := getAnomaliesByPolicyID(store, policy1ID)
	assert.Len(t, anomaliesForPolicy1, 1, "Expected 1 anomaly for policy1")
	if len(anomaliesForPolicy1) > 0 {
		assert.Equal(t, policy1ID, anomaliesForPolicy1[0].PolicyID, "Anomaly should be associated with policy1")
		assert.Equal(t, tx1ID, anomaliesForPolicy1[0].TransactionID, "Anomaly should reference tx1")
	}

	// Run matching for policy2 - should create anomaly for tx2 only
	results2, err := svc.MatchTransactions(ctx, policy2ID.String(), []string{tx2ID.String()})
	require.NoError(t, err)
	require.Len(t, results2, 1)
	assert.Equal(t, "UNMATCHED", results2[0].Decision)

	// Verify anomaly was created for policy2
	anomaliesForPolicy2 := getAnomaliesByPolicyID(store, policy2ID)
	assert.Len(t, anomaliesForPolicy2, 1, "Expected 1 anomaly for policy2")
	if len(anomaliesForPolicy2) > 0 {
		assert.Equal(t, policy2ID, anomaliesForPolicy2[0].PolicyID, "Anomaly should be associated with policy2")
		assert.Equal(t, tx2ID, anomaliesForPolicy2[0].TransactionID, "Anomaly should reference tx2")
	}

	// Critical assertion: policy1's anomalies should NOT include policy2's anomalies
	assert.NotEqual(t, anomaliesForPolicy1, anomaliesForPolicy2, "Each policy should have its own distinct anomalies")

	// Verify total anomalies
	assert.Len(t, store.anomalies, 2, "Expected exactly 2 anomalies total (one per policy)")
}

// TestPolicyFiltering_SameProviderDifferentPolicies tests the problematic scenario
// where multiple policies use the same provider. This test documents the CURRENT behavior
// which may need to be fixed.
func TestPolicyFiltering_SameProviderDifferentPolicies(t *testing.T) {
	ctx := context.Background()
	store := newMockStoreForPolicyFilteringTest()

	// Create two policies with the SAME provider (this is the problematic case)
	sharedConnectorType := "shared-provider"

	policy1ID := uuid.New()
	policy1 := &models.Policy{
		ID:            policy1ID,
		Name:          "test-policy-same-provider-1",
		Mode:          "transactional",
		LedgerName:    "shared-ledger",
		ConnectorType: &sharedConnectorType,
	}
	store.policies[policy1ID] = policy1

	policy2ID := uuid.New()
	policy2 := &models.Policy{
		ID:            policy2ID,
		Name:          "test-policy-same-provider-2",
		Mode:          "transactional",
		LedgerName:    "shared-ledger",
		ConnectorType: &sharedConnectorType,
	}
	store.policies[policy2ID] = policy2

	// Create a shared transaction (belongs to both policies via provider)
	txID := uuid.New()
	tx := &models.Transaction{
		ID:         txID,
		PolicyID:   nil,
		Side:       models.TransactionSideLedger,
		Provider:   "shared-ledger",
		ExternalID: "shared-tx",
		Amount:     1000,
		Currency:   "USD",
		OccurredAt: time.Now().UTC().Add(-25 * time.Hour),
		IngestedAt: time.Now().UTC(),
	}
	store.transactions[txID] = tx
	store.transactionsByProvider["shared-ledger"] = append(store.transactionsByProvider["shared-ledger"], tx)

	txStore := &mockTxStoreForPolicyFilteringTest{transactions: store.transactions}
	svc := NewService(store, txStore, nil)

	// Run matching for policy1
	results1, err := svc.MatchTransactions(ctx, policy1ID.String(), []string{txID.String()})
	require.NoError(t, err)
	require.Len(t, results1, 1)

	// Run matching for policy2
	results2, err := svc.MatchTransactions(ctx, policy2ID.String(), []string{txID.String()})
	require.NoError(t, err)
	require.Len(t, results2, 1)

	// Document current behavior: this creates TWO anomalies (one per policy)
	// This might be the intended behavior or a bug depending on requirements
	t.Logf("Total anomalies created: %d", len(store.anomalies))
	t.Logf("Anomalies for policy1: %d", len(getAnomaliesByPolicyID(store, policy1ID)))
	t.Logf("Anomalies for policy2: %d", len(getAnomaliesByPolicyID(store, policy2ID)))

	// The current behavior creates separate anomalies for each policy
	// Each policy's anomaly list is filtered by policy_id in the API
	anomaliesP1 := getAnomaliesByPolicyID(store, policy1ID)
	anomaliesP2 := getAnomaliesByPolicyID(store, policy2ID)

	// Both policies should see their own anomaly for the same transaction
	assert.Len(t, anomaliesP1, 1, "Policy1 should have 1 anomaly")
	assert.Len(t, anomaliesP2, 1, "Policy2 should have 1 anomaly")

	// Both anomalies reference the same transaction but different policies
	if len(anomaliesP1) > 0 && len(anomaliesP2) > 0 {
		assert.Equal(t, txID, anomaliesP1[0].TransactionID)
		assert.Equal(t, txID, anomaliesP2[0].TransactionID)
		assert.NotEqual(t, anomaliesP1[0].PolicyID, anomaliesP2[0].PolicyID, "Anomalies should have different policy IDs")
	}
}

// Helper function to get anomalies filtered by policy ID
func getAnomaliesByPolicyID(store *mockStoreForPolicyFilteringTest, policyID uuid.UUID) []*models.Anomaly {
	var result []*models.Anomaly
	for _, a := range store.anomalies {
		if a.PolicyID == policyID {
			result = append(result, a)
		}
	}
	return result
}

// mockStoreForPolicyFilteringTest is a mock store for testing policy filtering
type mockStoreForPolicyFilteringTest struct {
	policies               map[uuid.UUID]*models.Policy
	transactions           map[uuid.UUID]*models.Transaction
	transactionsByProvider map[string][]*models.Transaction
	anomalies              map[uuid.UUID]*models.Anomaly
}

func newMockStoreForPolicyFilteringTest() *mockStoreForPolicyFilteringTest {
	return &mockStoreForPolicyFilteringTest{
		policies:               make(map[uuid.UUID]*models.Policy),
		transactions:           make(map[uuid.UUID]*models.Transaction),
		transactionsByProvider: make(map[string][]*models.Transaction),
		anomalies:              make(map[uuid.UUID]*models.Anomaly),
	}
}

func (m *mockStoreForPolicyFilteringTest) Ping() error { return nil }

func (m *mockStoreForPolicyFilteringTest) GetPolicy(ctx context.Context, id uuid.UUID) (*models.Policy, error) {
	if p, ok := m.policies[id]; ok {
		return p, nil
	}
	return nil, storage.ErrNotFound
}

func (m *mockStoreForPolicyFilteringTest) GetTransactionByID(ctx context.Context, id uuid.UUID) (*models.Transaction, error) {
	if tx, ok := m.transactions[id]; ok {
		return tx, nil
	}
	return nil, storage.ErrNotFound
}

func (m *mockStoreForPolicyFilteringTest) GetTransactionByExternalIDWithoutPolicy(ctx context.Context, side models.TransactionSide, externalID string) (*models.Transaction, error) {
	return nil, storage.ErrNotFound
}

func (m *mockStoreForPolicyFilteringTest) CreateAnomaly(ctx context.Context, anomaly *models.Anomaly) error {
	m.anomalies[anomaly.ID] = anomaly
	return nil
}

func (m *mockStoreForPolicyFilteringTest) CreateMatch(ctx context.Context, match *models.Match) error {
	return nil
}

func (m *mockStoreForPolicyFilteringTest) FindMatchByTransactionIDs(ctx context.Context, policyID uuid.UUID, txIDs []uuid.UUID) (*models.Match, error) {
	return nil, nil
}

func (m *mockStoreForPolicyFilteringTest) GetTransactionsByProvider(ctx context.Context, provider string, side models.TransactionSide) ([]models.Transaction, error) {
	var result []models.Transaction
	for _, tx := range m.transactionsByProvider[provider] {
		if tx.Side == side {
			result = append(result, *tx)
		}
	}
	return result, nil
}

// Implement remaining Store interface methods
func (m *mockStoreForPolicyFilteringTest) CreatePolicy(ctx context.Context, policy *models.Policy) error {
	return nil
}
func (m *mockStoreForPolicyFilteringTest) DeletePolicy(ctx context.Context, id uuid.UUID) error {
	return nil
}
func (m *mockStoreForPolicyFilteringTest) UpdatePolicy(ctx context.Context, policy *models.Policy) error {
	return nil
}
func (m *mockStoreForPolicyFilteringTest) ListPolicies(ctx context.Context, q storage.GetPoliciesQuery) (*bunpaginate.Cursor[models.Policy], error) {
	return nil, nil
}
func (m *mockStoreForPolicyFilteringTest) CreateReconciation(ctx context.Context, reco *models.Reconciliation) error {
	return nil
}
func (m *mockStoreForPolicyFilteringTest) GetReconciliation(ctx context.Context, id uuid.UUID) (*models.Reconciliation, error) {
	return nil, nil
}
func (m *mockStoreForPolicyFilteringTest) ListReconciliations(ctx context.Context, q storage.GetReconciliationsQuery) (*bunpaginate.Cursor[models.Reconciliation], error) {
	return nil, nil
}
func (m *mockStoreForPolicyFilteringTest) ListMatchesByPolicy(ctx context.Context, q storage.GetMatchesQuery) (*bunpaginate.Cursor[models.Match], error) {
	return nil, nil
}
func (m *mockStoreForPolicyFilteringTest) GetMatchByID(ctx context.Context, id uuid.UUID) (*models.Match, error) {
	return nil, nil
}
func (m *mockStoreForPolicyFilteringTest) CountMatchesByDecision(ctx context.Context, policyID uuid.UUID, decision models.Decision) (int64, error) {
	return 0, nil
}
func (m *mockStoreForPolicyFilteringTest) ListAnomaliesByPolicy(ctx context.Context, q storage.GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error) {
	var data []models.Anomaly
	if q.Options.Options.PolicyID != nil {
		for _, a := range m.anomalies {
			if a.PolicyID == *q.Options.Options.PolicyID {
				data = append(data, *a)
			}
		}
	} else {
		for _, a := range m.anomalies {
			data = append(data, *a)
		}
	}
	return &bunpaginate.Cursor[models.Anomaly]{Data: data}, nil
}
func (m *mockStoreForPolicyFilteringTest) GetAnomalyByID(ctx context.Context, id uuid.UUID) (*models.Anomaly, error) {
	return nil, nil
}
func (m *mockStoreForPolicyFilteringTest) FindOpenAnomaliesByTransactionIDs(ctx context.Context, transactionIDs []uuid.UUID) ([]models.Anomaly, error) {
	return nil, nil
}
func (m *mockStoreForPolicyFilteringTest) ResolveAnomaly(ctx context.Context, id uuid.UUID, resolvedBy string) error {
	return nil
}
func (m *mockStoreForPolicyFilteringTest) GetLatestReportByPolicyID(ctx context.Context, policyID uuid.UUID) (*models.Report, error) {
	return nil, nil
}
func (m *mockStoreForPolicyFilteringTest) DeleteTransactionsByProvider(ctx context.Context, provider string, side models.TransactionSide) (int64, error) {
	return 0, nil
}
func (m *mockStoreForPolicyFilteringTest) CreateBackfill(ctx context.Context, backfill *models.Backfill) error {
	return nil
}
func (m *mockStoreForPolicyFilteringTest) GetBackfill(ctx context.Context, id uuid.UUID) (*models.Backfill, error) {
	return nil, nil
}
func (m *mockStoreForPolicyFilteringTest) UpdateBackfillStatus(ctx context.Context, id uuid.UUID, status models.BackfillStatus, errorMessage *string) error {
	return nil
}
func (m *mockStoreForPolicyFilteringTest) UpdateBackfillProgress(ctx context.Context, id uuid.UUID, ingested int64, lastCursor *string) error {
	return nil
}

var _ Store = (*mockStoreForPolicyFilteringTest)(nil)

// mockTxStoreForPolicyFilteringTest implements storage.TransactionStore for testing.
type mockTxStoreForPolicyFilteringTest struct {
	transactions map[uuid.UUID]*models.Transaction
}

func (m *mockTxStoreForPolicyFilteringTest) Create(ctx context.Context, tx *models.Transaction) error {
	return nil
}
func (m *mockTxStoreForPolicyFilteringTest) CreateBatch(ctx context.Context, txs []*models.Transaction) ([]bool, error) {
	results := make([]bool, len(txs))
	for i := range results {
		results[i] = true
	}
	return results, nil
}
func (m *mockTxStoreForPolicyFilteringTest) GetByID(ctx context.Context, id uuid.UUID) (*models.Transaction, error) {
	if tx, ok := m.transactions[id]; ok {
		return tx, nil
	}
	return nil, storage.ErrNotFound
}
func (m *mockTxStoreForPolicyFilteringTest) GetByExternalID(ctx context.Context, side models.TransactionSide, externalID string) (*models.Transaction, error) {
	return nil, storage.ErrNotFound
}
func (m *mockTxStoreForPolicyFilteringTest) GetByProvider(ctx context.Context, provider string, side models.TransactionSide) ([]*models.Transaction, error) {
	return nil, nil
}
func (m *mockTxStoreForPolicyFilteringTest) SearchByField(ctx context.Context, side models.TransactionSide, field string, value string) ([]*models.Transaction, error) {
	return nil, nil
}
func (m *mockTxStoreForPolicyFilteringTest) ExistsByExternalIDs(ctx context.Context, side models.TransactionSide, externalIDs []string) (map[string]bool, error) {
	return nil, nil
}
func (m *mockTxStoreForPolicyFilteringTest) Refresh(ctx context.Context) error {
	return nil
}

var _ storage.TransactionStore = (*mockTxStoreForPolicyFilteringTest)(nil)
