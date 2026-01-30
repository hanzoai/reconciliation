package anomaly

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/formancehq/reconciliation/internal/matching"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDefaultResolver_ResolveForMatch(t *testing.T) {
	t.Run("resolves anomaly when match created for transaction with open anomaly", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockAnomalyRepo := storage.NewMockAnomalyRepository(ctrl)

		policyID := uuid.New()
		ledgerTxID := uuid.New()
		paymentsTxID := uuid.New()
		matchID := uuid.New()
		anomalyID := uuid.New()

		match := &models.Match{
			ID:                     matchID,
			PolicyID:               &policyID,
			LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
			PaymentsTransactionIDs: []uuid.UUID{paymentsTxID},
			Score:                  1.0,
			Decision:               models.DecisionMatched,
			CreatedAt:              time.Now().UTC(),
		}

		openAnomaly := models.Anomaly{
			ID:            anomalyID,
			PolicyID:               &policyID,
			TransactionID: &ledgerTxID,
			Type:          models.AnomalyTypeMissingOnPayments,
			Severity:      models.SeverityHigh,
			State:         models.AnomalyStateOpen,
			Reason:        "Test anomaly",
			CreatedAt:     time.Now().UTC(),
		}

		// Expect FindOpenByTransactionIDs to be called with both transaction IDs
		mockAnomalyRepo.EXPECT().
			FindOpenByTransactionIDs(gomock.Any(), []uuid.UUID{ledgerTxID, paymentsTxID}).
			Return([]models.Anomaly{openAnomaly}, nil)

		// Expect Resolve to be called with the anomaly ID and proper reason
		expectedReason := "Resolved by match " + matchID.String()
		mockAnomalyRepo.EXPECT().
			Resolve(gomock.Any(), anomalyID, expectedReason).
			Return(nil)

		resolver := NewDefaultResolver(mockAnomalyRepo)

		err := resolver.ResolveForMatch(context.Background(), match)
		require.NoError(t, err)
	})

	t.Run("does nothing when match created for transaction without anomaly", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockAnomalyRepo := storage.NewMockAnomalyRepository(ctrl)

		policyID := uuid.New()
		ledgerTxID := uuid.New()
		paymentsTxID := uuid.New()
		matchID := uuid.New()

		match := &models.Match{
			ID:                     matchID,
			PolicyID:               &policyID,
			LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
			PaymentsTransactionIDs: []uuid.UUID{paymentsTxID},
			Score:                  1.0,
			Decision:               models.DecisionMatched,
			CreatedAt:              time.Now().UTC(),
		}

		// Expect FindOpenByTransactionIDs to be called and return empty list
		mockAnomalyRepo.EXPECT().
			FindOpenByTransactionIDs(gomock.Any(), []uuid.UUID{ledgerTxID, paymentsTxID}).
			Return([]models.Anomaly{}, nil)

		// Resolve should NOT be called since there are no anomalies

		resolver := NewDefaultResolver(mockAnomalyRepo)

		err := resolver.ResolveForMatch(context.Background(), match)
		require.NoError(t, err)
	})

	t.Run("resolves multiple anomalies when match involves multiple transactions with anomalies", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockAnomalyRepo := storage.NewMockAnomalyRepository(ctrl)

		policyID := uuid.New()
		ledgerTxID := uuid.New()
		paymentsTxID := uuid.New()
		matchID := uuid.New()
		anomalyID1 := uuid.New()
		anomalyID2 := uuid.New()

		match := &models.Match{
			ID:                     matchID,
			PolicyID:               &policyID,
			LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
			PaymentsTransactionIDs: []uuid.UUID{paymentsTxID},
			Score:                  1.0,
			Decision:               models.DecisionMatched,
			CreatedAt:              time.Now().UTC(),
		}

		// Both transactions have anomalies
		anomalies := []models.Anomaly{
			{
				ID:            anomalyID1,
				PolicyID:               &policyID,
				TransactionID: &ledgerTxID,
				Type:          models.AnomalyTypeMissingOnPayments,
				Severity:      models.SeverityHigh,
				State:         models.AnomalyStateOpen,
			},
			{
				ID:            anomalyID2,
				PolicyID:               &policyID,
				TransactionID: &paymentsTxID,
				Type:          models.AnomalyTypeMissingOnLedger,
				Severity:      models.SeverityCritical,
				State:         models.AnomalyStateOpen,
			},
		}

		mockAnomalyRepo.EXPECT().
			FindOpenByTransactionIDs(gomock.Any(), []uuid.UUID{ledgerTxID, paymentsTxID}).
			Return(anomalies, nil)

		expectedReason := "Resolved by match " + matchID.String()
		// Both anomalies should be resolved
		mockAnomalyRepo.EXPECT().
			Resolve(gomock.Any(), anomalyID1, expectedReason).
			Return(nil)
		mockAnomalyRepo.EXPECT().
			Resolve(gomock.Any(), anomalyID2, expectedReason).
			Return(nil)

		resolver := NewDefaultResolver(mockAnomalyRepo)

		err := resolver.ResolveForMatch(context.Background(), match)
		require.NoError(t, err)
	})

	t.Run("reason contains match_id", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockAnomalyRepo := storage.NewMockAnomalyRepository(ctrl)

		policyID := uuid.New()
		ledgerTxID := uuid.New()
		matchID := uuid.New()
		anomalyID := uuid.New()

		match := &models.Match{
			ID:                     matchID,
			PolicyID:               &policyID,
			LedgerTransactionIDs:   []uuid.UUID{ledgerTxID},
			PaymentsTransactionIDs: []uuid.UUID{},
			Score:                  1.0,
			Decision:               models.DecisionMatched,
		}

		openAnomaly := models.Anomaly{
			ID:            anomalyID,
			TransactionID: &ledgerTxID,
			State:         models.AnomalyStateOpen,
		}

		mockAnomalyRepo.EXPECT().
			FindOpenByTransactionIDs(gomock.Any(), []uuid.UUID{ledgerTxID}).
			Return([]models.Anomaly{openAnomaly}, nil)

		// Verify the reason contains the match ID
		mockAnomalyRepo.EXPECT().
			Resolve(gomock.Any(), anomalyID, gomock.Any()).
			DoAndReturn(func(_ context.Context, id uuid.UUID, resolvedBy string) error {
				assert.Contains(t, resolvedBy, matchID.String())
				assert.Contains(t, resolvedBy, "Resolved by match")
				return nil
			})

		resolver := NewDefaultResolver(mockAnomalyRepo)

		err := resolver.ResolveForMatch(context.Background(), match)
		require.NoError(t, err)
	})

	t.Run("returns nil for nil match", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockAnomalyRepo := storage.NewMockAnomalyRepository(ctrl)

		// No repository calls expected for nil match

		resolver := NewDefaultResolver(mockAnomalyRepo)

		err := resolver.ResolveForMatch(context.Background(), nil)
		require.NoError(t, err)
	})

	t.Run("returns nil for match with no transaction IDs", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockAnomalyRepo := storage.NewMockAnomalyRepository(ctrl)

		policyID := uuid.New()
		match := &models.Match{
			ID:                     uuid.New(),
			PolicyID:               &policyID,
			LedgerTransactionIDs:   []uuid.UUID{},
			PaymentsTransactionIDs: []uuid.UUID{},
			Score:                  1.0,
		}

		// No repository calls expected for empty transaction IDs

		resolver := NewDefaultResolver(mockAnomalyRepo)

		err := resolver.ResolveForMatch(context.Background(), match)
		require.NoError(t, err)
	})

	t.Run("returns error when FindOpenByTransactionIDs fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockAnomalyRepo := storage.NewMockAnomalyRepository(ctrl)

		policyID := uuid.New()
		match := &models.Match{
			ID:                     uuid.New(),
			PolicyID:               &policyID,
			LedgerTransactionIDs:   []uuid.UUID{uuid.New()},
			PaymentsTransactionIDs: []uuid.UUID{},
		}

		mockAnomalyRepo.EXPECT().
			FindOpenByTransactionIDs(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("database error"))

		resolver := NewDefaultResolver(mockAnomalyRepo)

		err := resolver.ResolveForMatch(context.Background(), match)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to find open anomalies")
	})

	t.Run("returns error when Resolve fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockAnomalyRepo := storage.NewMockAnomalyRepository(ctrl)

		anomalyID := uuid.New()
		policyID := uuid.New()
		match := &models.Match{
			ID:                     uuid.New(),
			PolicyID:               &policyID,
			LedgerTransactionIDs:   []uuid.UUID{uuid.New()},
			PaymentsTransactionIDs: []uuid.UUID{},
		}

		openAnomaly := models.Anomaly{
			ID:    anomalyID,
			State: models.AnomalyStateOpen,
		}

		mockAnomalyRepo.EXPECT().
			FindOpenByTransactionIDs(gomock.Any(), gomock.Any()).
			Return([]models.Anomaly{openAnomaly}, nil)

		mockAnomalyRepo.EXPECT().
			Resolve(gomock.Any(), anomalyID, gomock.Any()).
			Return(errors.New("resolve failed"))

		resolver := NewDefaultResolver(mockAnomalyRepo)

		err := resolver.ResolveForMatch(context.Background(), match)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to resolve anomaly")
		assert.Contains(t, err.Error(), anomalyID.String())
	})
}

func TestResolver_Interface(t *testing.T) {
	t.Run("DefaultResolver implements matching.AnomalyResolver interface", func(t *testing.T) {
		var _ matching.AnomalyResolver = (*DefaultResolver)(nil)
	})
}
