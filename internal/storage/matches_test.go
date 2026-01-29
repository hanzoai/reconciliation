package storage

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// createTestPolicyForMatch creates a test policy in the database.
func createTestPolicyForMatch(t *testing.T, store *Storage) *models.Policy {
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

func createTestMatch(policyID uuid.UUID, ledgerTxIDs, paymentTxIDs []uuid.UUID) *models.Match {
	return &models.Match{
		ID:                     uuid.New(),
		PolicyID:               policyID,
		LedgerTransactionIDs:   ledgerTxIDs,
		PaymentsTransactionIDs: paymentTxIDs,
		Score:                  0.95,
		Decision:               models.DecisionMatched,
		Explanation: models.Explanation{
			Reason: "Amount and date match",
			Score:  0.95,
			FieldNotes: map[string]string{
				"amount": "exact match",
				"date":   "within tolerance",
			},
			Details: map[string]interface{}{
				"amountDiff": 0,
				"dateDiff":   "2h",
			},
		},
		CreatedAt: time.Now().UTC(),
	}
}

func TestMatchCreate(t *testing.T) {
	store := newStore(t)
	policy := createTestPolicyForMatch(t, store)

	t.Run("success with UUID arrays", func(t *testing.T) {
		ledgerTxIDs := []uuid.UUID{uuid.New(), uuid.New()}
		paymentTxIDs := []uuid.UUID{uuid.New()}

		match := createTestMatch(policy.ID, ledgerTxIDs, paymentTxIDs)
		err := store.CreateMatch(context.Background(), match)
		require.NoError(t, err)

		// Verify the match was created by fetching it
		fetched, err := store.GetMatchByID(context.Background(), match.ID)
		require.NoError(t, err)
		require.Equal(t, match.ID, fetched.ID)
		require.Equal(t, match.PolicyID, fetched.PolicyID)
		require.Equal(t, match.Score, fetched.Score)
		require.Equal(t, match.Decision, fetched.Decision)

		// Verify UUID arrays
		require.Len(t, fetched.LedgerTransactionIDs, 2)
		require.Equal(t, ledgerTxIDs[0], fetched.LedgerTransactionIDs[0])
		require.Equal(t, ledgerTxIDs[1], fetched.LedgerTransactionIDs[1])
		require.Len(t, fetched.PaymentsTransactionIDs, 1)
		require.Equal(t, paymentTxIDs[0], fetched.PaymentsTransactionIDs[0])
	})

	t.Run("duplicate ID returns ErrDuplicate", func(t *testing.T) {
		ledgerTxIDs := []uuid.UUID{uuid.New()}
		paymentTxIDs := []uuid.UUID{uuid.New()}

		match := createTestMatch(policy.ID, ledgerTxIDs, paymentTxIDs)
		err := store.CreateMatch(context.Background(), match)
		require.NoError(t, err)

		// Try to create a duplicate with same ID
		duplicate := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
		duplicate.ID = match.ID
		err = store.CreateMatch(context.Background(), duplicate)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrDuplicateKeyValue)
	})
}

func TestMatchGetByID(t *testing.T) {
	store := newStore(t)
	policy := createTestPolicyForMatch(t, store)

	t.Run("found with explanation JSONB", func(t *testing.T) {
		ledgerTxIDs := []uuid.UUID{uuid.New()}
		paymentTxIDs := []uuid.UUID{uuid.New()}

		match := createTestMatch(policy.ID, ledgerTxIDs, paymentTxIDs)
		err := store.CreateMatch(context.Background(), match)
		require.NoError(t, err)

		fetched, err := store.GetMatchByID(context.Background(), match.ID)
		require.NoError(t, err)
		require.Equal(t, match.ID, fetched.ID)

		// Verify Explanation JSONB fields
		require.Equal(t, "Amount and date match", fetched.Explanation.Reason)
		require.Equal(t, 0.95, fetched.Explanation.Score)
		require.Equal(t, "exact match", fetched.Explanation.FieldNotes["amount"])
		require.Equal(t, "within tolerance", fetched.Explanation.FieldNotes["date"])
		require.Equal(t, float64(0), fetched.Explanation.Details["amountDiff"])
		require.Equal(t, "2h", fetched.Explanation.Details["dateDiff"])
	})

	t.Run("not found returns ErrNotFound", func(t *testing.T) {
		_, err := store.GetMatchByID(context.Background(), uuid.New())
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestMatchListByPolicy(t *testing.T) {
	store := newStore(t)
	policy := createTestPolicyForMatch(t, store)

	// Create matches with different decisions
	match1 := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
	match1.Decision = models.DecisionMatched
	match1.CreatedAt = time.Now().UTC().Add(-3 * time.Hour)

	match2 := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
	match2.Decision = models.DecisionRequiresReview
	match2.CreatedAt = time.Now().UTC().Add(-2 * time.Hour)

	match3 := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
	match3.Decision = models.DecisionMatched
	match3.CreatedAt = time.Now().UTC().Add(-1 * time.Hour)

	for _, m := range []*models.Match{match1, match2, match3} {
		err := store.CreateMatch(context.Background(), m)
		require.NoError(t, err)
	}

	t.Run("filtering by decision", func(t *testing.T) {
		decision := models.DecisionMatched
		policyID := policy.ID
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID: &policyID,
			Decision: &decision,
		}))
		cursor, err := store.ListMatchesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		for _, m := range cursor.Data {
			require.Equal(t, models.DecisionMatched, m.Decision)
		}
	})

	t.Run("filter by policyID only", func(t *testing.T) {
		policyID := policy.ID
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID: &policyID,
		}))
		cursor, err := store.ListMatchesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 3)
		for _, m := range cursor.Data {
			require.Equal(t, policy.ID, m.PolicyID)
		}
	})

	t.Run("pagination works", func(t *testing.T) {
		policyID := policy.ID
		// First page
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID: &policyID,
		}).WithPageSize(2))
		cursor, err := store.ListMatchesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 2)
		require.True(t, cursor.HasMore)

		// Second page
		q2 := GetMatchesQuery{
			PageSize: 2,
			Offset:   2,
		}
		cursor2, err := store.ListMatchesByPolicy(context.Background(), q2)
		require.NoError(t, err)
		require.Len(t, cursor2.Data, 1)
		require.False(t, cursor2.HasMore)
	})

	t.Run("empty result with unknown policy", func(t *testing.T) {
		unknownPolicyID := uuid.New()
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID: &unknownPolicyID,
		}))
		cursor, err := store.ListMatchesByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.Len(t, cursor.Data, 0)
	})

	t.Run("filtering by score_min", func(t *testing.T) {
		// Create matches with different scores
		highScoreMatch := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
		highScoreMatch.Score = 0.95
		err := store.CreateMatch(context.Background(), highScoreMatch)
		require.NoError(t, err)

		lowScoreMatch := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
		lowScoreMatch.Score = 0.5
		err = store.CreateMatch(context.Background(), lowScoreMatch)
		require.NoError(t, err)

		policyID := policy.ID
		scoreMin := 0.8
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID: &policyID,
			ScoreMin: &scoreMin,
		}))
		cursor, err := store.ListMatchesByPolicy(context.Background(), q)
		require.NoError(t, err)
		for _, m := range cursor.Data {
			require.GreaterOrEqual(t, m.Score, 0.8)
		}
	})

	t.Run("filtering by created_after", func(t *testing.T) {
		policyID := policy.ID
		afterTime := time.Now().UTC().Add(-90 * time.Minute) // Should include match3 only (created 1 hour ago)
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID:     &policyID,
			CreatedAfter: &afterTime,
		}))
		cursor, err := store.ListMatchesByPolicy(context.Background(), q)
		require.NoError(t, err)
		for _, m := range cursor.Data {
			require.True(t, m.CreatedAt.After(afterTime))
		}
	})

	t.Run("filtering by created_before", func(t *testing.T) {
		policyID := policy.ID
		beforeTime := time.Now().UTC().Add(-150 * time.Minute) // Should include match1 only (created 3 hours ago)
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID:      &policyID,
			CreatedBefore: &beforeTime,
		}))
		cursor, err := store.ListMatchesByPolicy(context.Background(), q)
		require.NoError(t, err)
		for _, m := range cursor.Data {
			require.True(t, m.CreatedAt.Before(beforeTime))
		}
	})

	t.Run("combined filters", func(t *testing.T) {
		policyID := policy.ID
		decision := models.DecisionMatched
		scoreMin := 0.9
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID: &policyID,
			Decision: &decision,
			ScoreMin: &scoreMin,
		}))
		cursor, err := store.ListMatchesByPolicy(context.Background(), q)
		require.NoError(t, err)
		for _, m := range cursor.Data {
			require.Equal(t, models.DecisionMatched, m.Decision)
			require.GreaterOrEqual(t, m.Score, 0.9)
		}
	})
}

func TestMatchUpdateDecision(t *testing.T) {
	store := newStore(t)
	policy := createTestPolicyForMatch(t, store)

	t.Run("atomic update", func(t *testing.T) {
		match := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
		match.Decision = models.DecisionRequiresReview
		err := store.CreateMatch(context.Background(), match)
		require.NoError(t, err)

		// Update decision
		err = store.UpdateMatchDecision(context.Background(), match.ID, models.DecisionManual)
		require.NoError(t, err)

		// Verify the update
		fetched, err := store.GetMatchByID(context.Background(), match.ID)
		require.NoError(t, err)
		require.Equal(t, models.DecisionManual, fetched.Decision)

		// Verify other fields unchanged
		require.Equal(t, match.PolicyID, fetched.PolicyID)
		require.Equal(t, match.Score, fetched.Score)
		require.Equal(t, match.Explanation.Reason, fetched.Explanation.Reason)
	})

	t.Run("not found returns ErrNotFound", func(t *testing.T) {
		err := store.UpdateMatchDecision(context.Background(), uuid.New(), models.DecisionMatched)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNotFound)
	})
}

func TestMatchListPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance test in short mode")
	}

	store := newStore(t)
	policy := createTestPolicyForMatch(t, store)

	// Create 1000 matches
	const numMatches = 1000
	for i := 0; i < numMatches; i++ {
		match := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
		match.Score = float64(i%100) / 100.0 // Varying scores
		switch i % 3 {
		case 0:
			match.Decision = models.DecisionMatched
		case 1:
			match.Decision = models.DecisionRequiresReview
		default:
			match.Decision = models.DecisionUnmatched
		}
		err := store.CreateMatch(context.Background(), match)
		require.NoError(t, err)
	}

	t.Run("list 1000 matches in under 200ms", func(t *testing.T) {
		policyID := policy.ID
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID: &policyID,
		}).WithPageSize(1000))

		start := time.Now()
		cursor, err := store.ListMatchesByPolicy(context.Background(), q)
		elapsed := time.Since(start)

		require.NoError(t, err)
		require.Len(t, cursor.Data, numMatches)
		require.Less(t, elapsed, 200*time.Millisecond, "expected query to complete in under 200ms, took %v", elapsed)
		t.Logf("Listed %d matches in %v", numMatches, elapsed)
	})
}

func TestMatchRepository(t *testing.T) {
	store := newStore(t)
	repo := NewMatchRepository(store)
	policy := createTestPolicyForMatch(t, store)

	t.Run("implements interface", func(t *testing.T) {
		var _ = repo
	})

	t.Run("Create and GetByID", func(t *testing.T) {
		match := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
		err := repo.Create(context.Background(), match)
		require.NoError(t, err)

		fetched, err := repo.GetByID(context.Background(), match.ID)
		require.NoError(t, err)
		require.Equal(t, match.ID, fetched.ID)
	})

	t.Run("ListByPolicy", func(t *testing.T) {
		policyID := policy.ID
		q := NewGetMatchesQuery(NewPaginatedQueryOptions(MatchesFilters{
			PolicyID: &policyID,
		}))
		cursor, err := repo.ListByPolicy(context.Background(), q)
		require.NoError(t, err)
		require.NotNil(t, cursor)
	})

	t.Run("UpdateDecision", func(t *testing.T) {
		match := createTestMatch(policy.ID, []uuid.UUID{uuid.New()}, []uuid.UUID{uuid.New()})
		match.Decision = models.DecisionRequiresReview
		err := repo.Create(context.Background(), match)
		require.NoError(t, err)

		err = repo.UpdateDecision(context.Background(), match.ID, models.DecisionMatched)
		require.NoError(t, err)

		fetched, err := repo.GetByID(context.Background(), match.ID)
		require.NoError(t, err)
		require.Equal(t, models.DecisionMatched, fetched.Decision)
	})
}
