package storage

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// createTestPolicyForAnomaly creates a test policy in the database.
func createTestPolicyForAnomaly(t *testing.T, store *Storage) *models.Policy {
	t.Helper()
	policy := &models.Policy{
		ID:         uuid.New(),
		CreatedAt:  time.Now().UTC(),
		Name:       "test-policy-" + uuid.NewString()[:8],
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
	return policy
}

func createTestAnomaly(policyID, transactionID uuid.UUID, anomalyType models.AnomalyType, severity models.Severity) *models.Anomaly {
	return &models.Anomaly{
		ID:            uuid.New(),
		PolicyID:      &policyID,
		TransactionID: &transactionID,
		Type:          anomalyType,
		Severity:      severity,
		State:         models.AnomalyStateOpen,
		Reason:        "Test anomaly reason",
		CreatedAt:     time.Now().UTC(),
	}
}

func TestAnomalyCreate(t *testing.T) {
	store := newStore(t)
	policy := createTestPolicyForAnomaly(t, store)
	// Transaction ID now references OpenSearch document, no FK constraint
	txID := uuid.New()

	t.Run("success", func(t *testing.T) {
		anomaly := createTestAnomaly(policy.ID, txID, models.AnomalyTypeMissingOnPayments, models.SeverityHigh)
		err := store.CreateAnomaly(context.Background(), anomaly)
		require.NoError(t, err)

		// Verify the anomaly was created by fetching it
		fetched, err := store.GetAnomalyByID(context.Background(), anomaly.ID)
		require.NoError(t, err)
		require.Equal(t, anomaly.ID, fetched.ID)
		require.NotNil(t, anomaly.PolicyID)
		require.NotNil(t, fetched.PolicyID)
		require.Equal(t, *anomaly.PolicyID, *fetched.PolicyID)
		require.NotNil(t, anomaly.TransactionID)
		require.NotNil(t, fetched.TransactionID)
		require.Equal(t, *anomaly.TransactionID, *fetched.TransactionID)
		require.Equal(t, anomaly.Type, fetched.Type)
		require.Equal(t, anomaly.Severity, fetched.Severity)
		require.Equal(t, anomaly.State, fetched.State)
		require.Equal(t, anomaly.Reason, fetched.Reason)
	})

	t.Run("duplicate ID returns ErrDuplicate", func(t *testing.T) {
		anomaly := createTestAnomaly(policy.ID, txID, models.AnomalyTypeMissingOnLedger, models.SeverityCritical)
		err := store.CreateAnomaly(context.Background(), anomaly)
		require.NoError(t, err)

		// Try to create a duplicate with same ID
		duplicate := createTestAnomaly(policy.ID, txID, models.AnomalyTypeAmountMismatch, models.SeverityMedium)
		duplicate.ID = anomaly.ID
		err = store.CreateAnomaly(context.Background(), duplicate)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDuplicateKeyValue)
	})
}

func TestAnomalyGetByID(t *testing.T) {
	store := newStore(t)
	policy := createTestPolicyForAnomaly(t, store)
	txID := uuid.New()

	t.Run("found", func(t *testing.T) {
		anomaly := createTestAnomaly(policy.ID, txID, models.AnomalyTypeDuplicateLedger, models.SeverityLow)
		err := store.CreateAnomaly(context.Background(), anomaly)
		require.NoError(t, err)

		fetched, err := store.GetAnomalyByID(context.Background(), anomaly.ID)
		require.NoError(t, err)
		require.Equal(t, anomaly.ID, fetched.ID)
		require.Equal(t, anomaly.Type, fetched.Type)
		require.Equal(t, anomaly.Severity, fetched.Severity)
	})

	t.Run("not found returns ErrNotFound", func(t *testing.T) {
		_, err := store.GetAnomalyByID(context.Background(), uuid.New())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestAnomalyListByPolicy(t *testing.T) {
	store := newStore(t)
	policy := createTestPolicyForAnomaly(t, store)
	txID := uuid.New()

	// Create anomalies with different types and states
	anomaly1 := createTestAnomaly(policy.ID, txID, models.AnomalyTypeMissingOnPayments, models.SeverityHigh)
	anomaly1.State = models.AnomalyStateOpen
	anomaly1.CreatedAt = time.Now().UTC().Add(-3 * time.Hour)

	anomaly2 := createTestAnomaly(policy.ID, txID, models.AnomalyTypeMissingOnLedger, models.SeverityCritical)
	anomaly2.State = models.AnomalyStateResolved
	anomaly2.CreatedAt = time.Now().UTC().Add(-2 * time.Hour)

	anomaly3 := createTestAnomaly(policy.ID, txID, models.AnomalyTypeMissingOnPayments, models.SeverityMedium)
	anomaly3.State = models.AnomalyStateOpen
	anomaly3.CreatedAt = time.Now().UTC().Add(-1 * time.Hour)

	for _, a := range []*models.Anomaly{anomaly1, anomaly2, anomaly3} {
		err := store.CreateAnomaly(context.Background(), a)
		require.NoError(t, err)
	}

	t.Run("filtering by state", func(t *testing.T) {
		state := models.AnomalyStateOpen
		policyID := policy.ID
		q := NewGetAnomaliesQuery(NewPaginatedQueryOptions(AnomaliesFilters{
			PolicyID: &policyID,
			State:    &state,
		}))
		cursor, err := store.ListAnomaliesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		for _, a := range cursor.Data {
			require.Equal(t, models.AnomalyStateOpen, a.State)
		}
	})

	t.Run("filtering by type", func(t *testing.T) {
		anomalyType := models.AnomalyTypeMissingOnPayments
		policyID := policy.ID
		q := NewGetAnomaliesQuery(NewPaginatedQueryOptions(AnomaliesFilters{
			PolicyID: &policyID,
			Type:     &anomalyType,
		}))
		cursor, err := store.ListAnomaliesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		for _, a := range cursor.Data {
			require.Equal(t, models.AnomalyTypeMissingOnPayments, a.Type)
		}
	})

	t.Run("filtering by state and type", func(t *testing.T) {
		state := models.AnomalyStateOpen
		anomalyType := models.AnomalyTypeMissingOnPayments
		policyID := policy.ID
		q := NewGetAnomaliesQuery(NewPaginatedQueryOptions(AnomaliesFilters{
			PolicyID: &policyID,
			State:    &state,
			Type:     &anomalyType,
		}))
		cursor, err := store.ListAnomaliesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		for _, a := range cursor.Data {
			require.Equal(t, models.AnomalyStateOpen, a.State)
			require.Equal(t, models.AnomalyTypeMissingOnPayments, a.Type)
		}
	})

	t.Run("filter by policyID only", func(t *testing.T) {
		policyID := policy.ID
		q := NewGetAnomaliesQuery(NewPaginatedQueryOptions(AnomaliesFilters{
			PolicyID: &policyID,
		}))
		cursor, err := store.ListAnomaliesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 3)
		for _, a := range cursor.Data {
			require.NotNil(t, a.PolicyID)
			require.Equal(t, policy.ID, *a.PolicyID)
		}
	})

	t.Run("pagination works", func(t *testing.T) {
		policyID := policy.ID
		// First page
		q := NewGetAnomaliesQuery(NewPaginatedQueryOptions(AnomaliesFilters{
			PolicyID: &policyID,
		}).WithPageSize(2))
		cursor, err := store.ListAnomaliesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		require.True(t, cursor.HasMore)

		// Second page
		q2 := GetAnomaliesQuery{
			PageSize: 2,
			Offset:   2,
		}
		cursor2, err := store.ListAnomaliesByPolicy(context.Background(), q2)
		require.NoError(t, err)
		require.Len(t, cursor2.Data, 1)
		require.False(t, cursor2.HasMore)
	})

	t.Run("empty result with unknown policy", func(t *testing.T) {
		unknownPolicyID := uuid.New()
		q := NewGetAnomaliesQuery(NewPaginatedQueryOptions(AnomaliesFilters{
			PolicyID: &unknownPolicyID,
		}))
		cursor, err := store.ListAnomaliesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 0)
	})
}

func TestAnomalyResolve(t *testing.T) {
	store := newStore(t)
	policy := createTestPolicyForAnomaly(t, store)
	txID := uuid.New()

	t.Run("updates state resolved_at resolved_by", func(t *testing.T) {
		anomaly := createTestAnomaly(policy.ID, txID, models.AnomalyTypeMissingOnPayments, models.SeverityHigh)
		anomaly.State = models.AnomalyStateOpen
		err := store.CreateAnomaly(context.Background(), anomaly)
		require.NoError(t, err)

		// Verify initial state
		fetched, err := store.GetAnomalyByID(context.Background(), anomaly.ID)
		require.NoError(t, err)
		require.Equal(t, models.AnomalyStateOpen, fetched.State)
		require.Nil(t, fetched.ResolvedAt)
		require.Nil(t, fetched.ResolvedBy)

		// Resolve the anomaly
		resolvedBy := "test-user@example.com"
		err = store.ResolveAnomaly(context.Background(), anomaly.ID, resolvedBy)
		require.NoError(t, err)

		// Verify the update
		fetched, err = store.GetAnomalyByID(context.Background(), anomaly.ID)
		require.NoError(t, err)
		require.Equal(t, models.AnomalyStateResolved, fetched.State)
		require.NotNil(t, fetched.ResolvedAt)
		require.NotNil(t, fetched.ResolvedBy)
		require.Equal(t, resolvedBy, *fetched.ResolvedBy)

		// Verify other fields unchanged
		require.NotNil(t, anomaly.PolicyID)
		require.NotNil(t, fetched.PolicyID)
		require.Equal(t, *anomaly.PolicyID, *fetched.PolicyID)
		require.NotNil(t, anomaly.TransactionID)
		require.NotNil(t, fetched.TransactionID)
		require.Equal(t, *anomaly.TransactionID, *fetched.TransactionID)
		require.Equal(t, anomaly.Type, fetched.Type)
		require.Equal(t, anomaly.Severity, fetched.Severity)
		require.Equal(t, anomaly.Reason, fetched.Reason)
	})

	t.Run("not found returns ErrNotFound", func(t *testing.T) {
		err := store.ResolveAnomaly(context.Background(), uuid.New(), "test-user")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestAnomalyRepository(t *testing.T) {
	store := newStore(t)
	repo := NewAnomalyRepository(store)
	policy := createTestPolicyForAnomaly(t, store)
	txID := uuid.New()

	t.Run("implements interface", func(t *testing.T) {
		var _ = repo
	})

	t.Run("Create and GetByID", func(t *testing.T) {
		anomaly := createTestAnomaly(policy.ID, txID, models.AnomalyTypeMissingOnPayments, models.SeverityHigh)
		err := repo.Create(context.Background(), anomaly)
		require.NoError(t, err)

		fetched, err := repo.GetByID(context.Background(), anomaly.ID)
		require.NoError(t, err)
		require.Equal(t, anomaly.ID, fetched.ID)
	})

	t.Run("ListByPolicy", func(t *testing.T) {
		policyID := policy.ID
		q := NewGetAnomaliesQuery(NewPaginatedQueryOptions(AnomaliesFilters{
			PolicyID: &policyID,
		}))
		cursor, err := repo.ListByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.NotNil(t, cursor)
	})

	t.Run("Resolve", func(t *testing.T) {
		anomaly := createTestAnomaly(policy.ID, txID, models.AnomalyTypeMissingOnLedger, models.SeverityCritical)
		anomaly.State = models.AnomalyStateOpen
		err := repo.Create(context.Background(), anomaly)
		require.NoError(t, err)

		resolvedBy := "admin@example.com"
		err = repo.Resolve(context.Background(), anomaly.ID, resolvedBy)
		require.NoError(t, err)

		fetched, err := repo.GetByID(context.Background(), anomaly.ID)
		require.NoError(t, err)
		require.Equal(t, models.AnomalyStateResolved, fetched.State)
		require.NotNil(t, fetched.ResolvedAt)
		require.NotNil(t, fetched.ResolvedBy)
		require.Equal(t, resolvedBy, *fetched.ResolvedBy)
	})

	t.Run("FindOpenByTransactionIDs", func(t *testing.T) {
		// Create a new transaction ID for this test
		tx2ID := uuid.New()

		anomaly := createTestAnomaly(policy.ID, tx2ID, models.AnomalyTypeMissingOnPayments, models.SeverityHigh)
		anomaly.State = models.AnomalyStateOpen
		err := repo.Create(context.Background(), anomaly)
		require.NoError(t, err)

		// Should find the open anomaly
		anomalies, err := repo.FindOpenByTransactionIDs(context.Background(), []uuid.UUID{tx2ID})
		require.NoError(t, err)
		require.Len(t, anomalies, 1)
		require.Equal(t, anomaly.ID, anomalies[0].ID)
	})
}

func TestFindOpenAnomaliesByTransactionIDs(t *testing.T) {
	store := newStore(t)
	policy := createTestPolicyForAnomaly(t, store)
	tx1ID := uuid.New()
	tx2ID := uuid.New()
	tx3ID := uuid.New()

	t.Run("returns only open anomalies", func(t *testing.T) {
		// Create an open anomaly
		openAnomaly := createTestAnomaly(policy.ID, tx1ID, models.AnomalyTypeMissingOnPayments, models.SeverityHigh)
		openAnomaly.State = models.AnomalyStateOpen
		err := store.CreateAnomaly(context.Background(), openAnomaly)
		require.NoError(t, err)

		// Create a resolved anomaly
		resolvedAnomaly := createTestAnomaly(policy.ID, tx1ID, models.AnomalyTypeMissingOnLedger, models.SeverityCritical)
		resolvedAnomaly.State = models.AnomalyStateResolved
		err = store.CreateAnomaly(context.Background(), resolvedAnomaly)
		require.NoError(t, err)

		// Should only return the open anomaly
		anomalies, err := store.FindOpenAnomaliesByTransactionIDs(context.Background(), []uuid.UUID{tx1ID})
		require.NoError(t, err)
		require.Len(t, anomalies, 1)
		require.Equal(t, openAnomaly.ID, anomalies[0].ID)
		require.Equal(t, models.AnomalyStateOpen, anomalies[0].State)
	})

	t.Run("returns anomalies for multiple transaction IDs", func(t *testing.T) {
		// Create anomalies for tx2 and tx3
		anomaly2 := createTestAnomaly(policy.ID, tx2ID, models.AnomalyTypeMissingOnPayments, models.SeverityMedium)
		anomaly2.State = models.AnomalyStateOpen
		err := store.CreateAnomaly(context.Background(), anomaly2)
		require.NoError(t, err)

		anomaly3 := createTestAnomaly(policy.ID, tx3ID, models.AnomalyTypeMissingOnLedger, models.SeverityLow)
		anomaly3.State = models.AnomalyStateOpen
		err = store.CreateAnomaly(context.Background(), anomaly3)
		require.NoError(t, err)

		// Should return both anomalies
		anomalies, err := store.FindOpenAnomaliesByTransactionIDs(context.Background(), []uuid.UUID{tx2ID, tx3ID})
		require.NoError(t, err)
		require.Len(t, anomalies, 2)

		// Verify both are present
		var foundTx2, foundTx3 bool
		for _, a := range anomalies {
			if a.ID == anomaly2.ID {
				foundTx2 = true
			}
			if a.ID == anomaly3.ID {
				foundTx3 = true
			}
		}
		require.True(t, foundTx2, "should find anomaly for tx2")
		require.True(t, foundTx3, "should find anomaly for tx3")
	})

	t.Run("returns empty slice for empty input", func(t *testing.T) {
		anomalies, err := store.FindOpenAnomaliesByTransactionIDs(context.Background(), []uuid.UUID{})
		require.NoError(t, err)
		require.Len(t, anomalies, 0)
	})

	t.Run("returns empty slice for unknown transaction IDs", func(t *testing.T) {
		anomalies, err := store.FindOpenAnomaliesByTransactionIDs(context.Background(), []uuid.UUID{uuid.New(), uuid.New()})
		require.NoError(t, err)
		require.Len(t, anomalies, 0)
	})
}
