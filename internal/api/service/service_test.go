package service

import (
	"context"
	"math/big"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"

	"github.com/formancehq/formance-sdk-go/v3/pkg/models/operations"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/shared"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

func ptrUUID(u uuid.UUID) *uuid.UUID {
	return &u
}

type mockSDKFormanceClient struct {
	ledgerVersion    string
	ledgerBalances   map[string]*big.Int
	paymentsVersion  string
	paymentsBalances map[string]*big.Int
}

func newMockSDKFormanceClient(
	ledgerVersion string,
	ledgerBalances map[string]*big.Int,
	paymentsVersion string,
	paymentsBalances map[string]*big.Int,
) *mockSDKFormanceClient {
	return &mockSDKFormanceClient{
		ledgerVersion:    ledgerVersion,
		ledgerBalances:   ledgerBalances,
		paymentsVersion:  paymentsVersion,
		paymentsBalances: paymentsBalances,
	}
}

func (s *mockSDKFormanceClient) PaymentsgetServerInfo(ctx context.Context) (*operations.PaymentsgetServerInfoResponse, error) {
	return &operations.PaymentsgetServerInfoResponse{
		ServerInfo: &shared.ServerInfo{
			Version: s.paymentsVersion,
		},
		StatusCode: http.StatusOK,
	}, nil
}

func (s *mockSDKFormanceClient) GetPoolBalances(ctx context.Context, req operations.GetPoolBalancesRequest) (*operations.GetPoolBalancesResponse, error) {
	poolBalances := make([]shared.PoolBalance, 0, len(s.paymentsBalances))
	for assetCode, balance := range s.paymentsBalances {
		poolBalances = append(poolBalances, shared.PoolBalance{
			Amount: balance,
			Asset:  assetCode,
		})
	}

	return &operations.GetPoolBalancesResponse{
		PoolBalancesResponse: &shared.PoolBalancesResponse{
			Data: shared.PoolBalances{
				Balances: poolBalances,
			},
		},
		StatusCode: http.StatusOK,
	}, nil
}

func (s *mockSDKFormanceClient) V2GetInfo(ctx context.Context) (*operations.V2GetInfoResponse, error) {
	return &operations.V2GetInfoResponse{
		StatusCode: http.StatusOK,
		V2ConfigInfoResponse: &shared.V2ConfigInfoResponse{
			Version: s.ledgerVersion,
		},
	}, nil
}

func (s *mockSDKFormanceClient) V2GetBalancesAggregated(ctx context.Context, req operations.V2GetBalancesAggregatedRequest) (*operations.V2GetBalancesAggregatedResponse, error) {
	balances := make(map[string]*big.Int)
	for assetCode, balance := range s.ledgerBalances {
		balances[assetCode] = balance
	}

	return &operations.V2GetBalancesAggregatedResponse{
		StatusCode: http.StatusOK,
		V2AggregateBalancesResponse: &shared.V2AggregateBalancesResponse{
			Data: balances,
		},
	}, nil
}

func (s *mockSDKFormanceClient) V2ListLedgers(ctx context.Context, req operations.V2ListLedgersRequest) (*operations.V2ListLedgersResponse, error) {
	return nil, nil
}

func (s *mockSDKFormanceClient) V2ListTransactions(ctx context.Context, req operations.V2ListTransactionsRequest) (*operations.V2ListTransactionsResponse, error) {
	return nil, nil
}

func (s *mockSDKFormanceClient) ListPayments(ctx context.Context, req operations.ListPaymentsRequest) (*operations.ListPaymentsResponse, error) {
	return nil, nil
}

type mockStore struct {
}

func newMockStore() *mockStore {
	return &mockStore{}
}

func (s *mockStore) Ping() error {
	return nil
}

func (s *mockStore) CreatePolicy(ctx context.Context, policy *models.Policy) error {
	return nil
}

func (s *mockStore) DeletePolicy(ctx context.Context, id uuid.UUID) error {
	return nil
}

func (s *mockStore) GetPolicy(ctx context.Context, id uuid.UUID) (*models.Policy, error) {
	return &models.Policy{
		ID:             id,
		CreatedAt:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		Name:           "test",
		LedgerName:     "default",
		LedgerQuery:    map[string]interface{}{},
		PaymentsPoolID: uuid.New(),
		Mode:           "balance",
		Topology:       "1:1",
	}, nil
}

func (s *mockStore) UpdatePolicy(ctx context.Context, policy *models.Policy) error {
	return nil
}

func (s *mockStore) ListPolicies(ctx context.Context, q storage.GetPoliciesQuery) (*bunpaginate.Cursor[models.Policy], error) {
	return nil, nil
}

func (s *mockStore) CreateReconciation(ctx context.Context, reco *models.Reconciliation) error {
	return nil
}

func (s *mockStore) GetReconciliation(ctx context.Context, id uuid.UUID) (*models.Reconciliation, error) {
	return nil, nil
}

func (s *mockStore) ListReconciliations(ctx context.Context, q storage.GetReconciliationsQuery) (*bunpaginate.Cursor[models.Reconciliation], error) {
	return nil, nil
}

func (s *mockStore) ListMatchesByPolicy(ctx context.Context, q storage.GetMatchesQuery) (*bunpaginate.Cursor[models.Match], error) {
	return &bunpaginate.Cursor[models.Match]{
		PageSize: 100,
		HasMore:  false,
		Data:     []models.Match{},
	}, nil
}

func (s *mockStore) GetMatchByID(ctx context.Context, id uuid.UUID) (*models.Match, error) {
	return &models.Match{
		ID:        id,
		PolicyID:  uuid.New(),
		Score:     0.95,
		Decision:  models.DecisionMatched,
		CreatedAt: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

func (s *mockStore) ListAnomaliesByPolicy(ctx context.Context, q storage.GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error) {
	return &bunpaginate.Cursor[models.Anomaly]{
		PageSize: 15,
		HasMore:  false,
		Data:     []models.Anomaly{},
	}, nil
}

func (s *mockStore) GetAnomalyByID(ctx context.Context, id uuid.UUID) (*models.Anomaly, error) {
	return &models.Anomaly{
		ID:            id,
		PolicyID:      uuid.New(),
		TransactionID: uuid.New(),
		Type:          models.AnomalyTypeMissingOnPayments,
		Severity:      models.SeverityCritical,
		State:         models.AnomalyStateOpen,
		Reason:        "Test anomaly",
		CreatedAt:     time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

func (s *mockStore) GetTransactionByID(ctx context.Context, id uuid.UUID) (*models.Transaction, error) {
	return &models.Transaction{
		ID:         id,
		PolicyID:   ptrUUID(uuid.New()),
		Side:       models.TransactionSideLedger,
		Provider:   "test-provider",
		ExternalID: "test-external-id",
		Amount:     10000,
		Currency:   "USD",
		OccurredAt: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		IngestedAt: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

func (s *mockStore) GetTransactionByExternalIDWithoutPolicy(ctx context.Context, side models.TransactionSide, externalID string) (*models.Transaction, error) {
	return nil, storage.ErrNotFound
}

func (s *mockStore) CreateMatch(ctx context.Context, match *models.Match) error {
	return nil
}

func (s *mockStore) FindMatchByTransactionIDs(ctx context.Context, policyID uuid.UUID, txIDs []uuid.UUID) (*models.Match, error) {
	return nil, nil
}

func (s *mockStore) FindOpenAnomaliesByTransactionIDs(ctx context.Context, transactionIDs []uuid.UUID) ([]models.Anomaly, error) {
	return []models.Anomaly{}, nil
}

func (s *mockStore) ResolveAnomaly(ctx context.Context, id uuid.UUID, resolvedBy string) error {
	return nil
}

func (s *mockStore) GetLatestReportByPolicyID(ctx context.Context, policyID uuid.UUID) (*models.Report, error) {
	return &models.Report{
		ID:                uuid.New(),
		PolicyID:          policyID,
		PeriodStart:       time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:         time.Date(2021, 1, 31, 23, 59, 59, 0, time.UTC),
		TotalTransactions: 100,
		MatchedCount:      90,
		MatchRate:         0.9,
		AnomaliesByType:   map[string]int64{},
		GeneratedAt:       time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

func (s *mockStore) CountMatchesByDecision(ctx context.Context, policyID uuid.UUID, decision models.Decision) (int64, error) {
	return 0, nil
}

func (s *mockStore) CreateAnomaly(ctx context.Context, anomaly *models.Anomaly) error {
	return nil
}

func (s *mockStore) GetTransactionsByProvider(ctx context.Context, provider string, side models.TransactionSide) ([]models.Transaction, error) {
	return []models.Transaction{}, nil
}

func (s *mockStore) DeleteTransactionsByProvider(ctx context.Context, provider string, side models.TransactionSide) (int64, error) {
	return 0, nil
}
func (s *mockStore) CreateBackfill(ctx context.Context, backfill *models.Backfill) error {
	return nil
}
func (s *mockStore) GetBackfill(ctx context.Context, id uuid.UUID) (*models.Backfill, error) {
	return nil, nil
}
func (s *mockStore) UpdateBackfillStatus(ctx context.Context, id uuid.UUID, status models.BackfillStatus, errorMessage *string) error {
	return nil
}
func (s *mockStore) UpdateBackfillProgress(ctx context.Context, id uuid.UUID, ingested int64, lastCursor *string) error {
	return nil
}

var _ Store = (*mockStore)(nil)
