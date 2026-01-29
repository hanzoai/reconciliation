package service

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"

	sdk "github.com/formancehq/formance-sdk-go/v3"
	"github.com/formancehq/formance-sdk-go/v3/pkg/models/operations"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

// Store interface defines the PostgreSQL storage operations.
// Transaction storage is handled separately by TransactionStore (OpenSearch).
type Store interface {
	Ping() error
	CreatePolicy(ctx context.Context, policy *models.Policy) error
	DeletePolicy(ctx context.Context, id uuid.UUID) error
	GetPolicy(ctx context.Context, id uuid.UUID) (*models.Policy, error)
	UpdatePolicy(ctx context.Context, policy *models.Policy) error
	ListPolicies(ctx context.Context, q storage.GetPoliciesQuery) (*bunpaginate.Cursor[models.Policy], error)

	CreateReconciation(ctx context.Context, reco *models.Reconciliation) error
	GetReconciliation(ctx context.Context, id uuid.UUID) (*models.Reconciliation, error)
	ListReconciliations(ctx context.Context, q storage.GetReconciliationsQuery) (*bunpaginate.Cursor[models.Reconciliation], error)

	ListMatchesByPolicy(ctx context.Context, q storage.GetMatchesQuery) (*bunpaginate.Cursor[models.Match], error)
	GetMatchByID(ctx context.Context, id uuid.UUID) (*models.Match, error)
	CreateMatch(ctx context.Context, match *models.Match) error
	FindMatchByTransactionIDs(ctx context.Context, policyID uuid.UUID, txIDs []uuid.UUID) (*models.Match, error)
	CountMatchesByDecision(ctx context.Context, policyID uuid.UUID, decision models.Decision) (int64, error)

	ListAnomaliesByPolicy(ctx context.Context, q storage.GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error)
	GetAnomalyByID(ctx context.Context, id uuid.UUID) (*models.Anomaly, error)
	FindOpenAnomaliesByTransactionIDs(ctx context.Context, transactionIDs []uuid.UUID) ([]models.Anomaly, error)
	ResolveAnomaly(ctx context.Context, id uuid.UUID, resolvedBy string) error
	CreateAnomaly(ctx context.Context, anomaly *models.Anomaly) error

	GetLatestReportByPolicyID(ctx context.Context, policyID uuid.UUID) (*models.Report, error)

	CreateBackfill(ctx context.Context, backfill *models.Backfill) error
	GetBackfill(ctx context.Context, id uuid.UUID) (*models.Backfill, error)
	UpdateBackfillStatus(ctx context.Context, id uuid.UUID, status models.BackfillStatus, errorMessage *string) error
	UpdateBackfillProgress(ctx context.Context, id uuid.UUID, ingested int64, lastCursor *string) error
}

// Service provides the business logic for the reconciliation API.
// It uses:
// - Store (PostgreSQL) for policies, matchers, matches, anomalies, reconciliations
// - TransactionStore (OpenSearch) for transactions
type Service struct {
	store        Store
	txStore      storage.TransactionStore
	client       SDKFormance
	publisher    Publisher
	publishTopic string
}

// Publisher defines the interface for publishing events.
type Publisher interface {
	Publish(topic string, messages ...*message.Message) error
}

// NewService creates a new Service instance.
func NewService(store Store, txStore storage.TransactionStore, client SDKFormance) *Service {
	return &Service{
		store:   store,
		txStore: txStore,
		client:  client,
	}
}

// SetPublisher sets the event publisher for the service.
func (s *Service) SetPublisher(publisher Publisher, topic string) {
	s.publisher = publisher
	s.publishTopic = topic
}

type SDKFormance interface {
	PaymentsgetServerInfo(ctx context.Context) (*operations.PaymentsgetServerInfoResponse, error)
	GetPoolBalances(ctx context.Context, req operations.GetPoolBalancesRequest) (*operations.GetPoolBalancesResponse, error)
	V2GetInfo(ctx context.Context) (*operations.V2GetInfoResponse, error)
	V2GetBalancesAggregated(ctx context.Context, req operations.V2GetBalancesAggregatedRequest) (*operations.V2GetBalancesAggregatedResponse, error)
	V2ListLedgers(ctx context.Context, req operations.V2ListLedgersRequest) (*operations.V2ListLedgersResponse, error)
	V2ListTransactions(ctx context.Context, req operations.V2ListTransactionsRequest) (*operations.V2ListTransactionsResponse, error)
	ListPayments(ctx context.Context, req operations.ListPaymentsRequest) (*operations.ListPaymentsResponse, error)
}

type sdkFormanceClient struct {
	client *sdk.Formance
}

func NewSDKFormance(client *sdk.Formance) *sdkFormanceClient {
	return &sdkFormanceClient{
		client: client,
	}
}

func (s *sdkFormanceClient) PaymentsgetServerInfo(ctx context.Context) (*operations.PaymentsgetServerInfoResponse, error) {
	return s.client.Payments.V1.PaymentsgetServerInfo(ctx)
}

func (s *sdkFormanceClient) GetPoolBalances(ctx context.Context, req operations.GetPoolBalancesRequest) (*operations.GetPoolBalancesResponse, error) {
	return s.client.Payments.V1.GetPoolBalances(ctx, req)
}

func (s *sdkFormanceClient) V2GetInfo(ctx context.Context) (*operations.V2GetInfoResponse, error) {
	return s.client.Ledger.V2.GetInfo(ctx)
}

func (s *sdkFormanceClient) V2GetBalancesAggregated(ctx context.Context, req operations.V2GetBalancesAggregatedRequest) (*operations.V2GetBalancesAggregatedResponse, error) {
	return s.client.Ledger.V2.GetBalancesAggregated(ctx, req)
}

func (s *sdkFormanceClient) V2ListLedgers(ctx context.Context, req operations.V2ListLedgersRequest) (*operations.V2ListLedgersResponse, error) {
	return s.client.Ledger.V2.ListLedgers(ctx, req)
}

func (s *sdkFormanceClient) V2ListTransactions(ctx context.Context, req operations.V2ListTransactionsRequest) (*operations.V2ListTransactionsResponse, error) {
	return s.client.Ledger.V2.ListTransactions(ctx, req)
}

func (s *sdkFormanceClient) ListPayments(ctx context.Context, req operations.ListPaymentsRequest) (*operations.ListPaymentsResponse, error) {
	return s.client.Payments.V1.ListPayments(ctx, req)
}

var _ SDKFormance = (*sdkFormanceClient)(nil)

// GetTransactionByID retrieves a transaction by its UUID from OpenSearch.
func (s *Service) GetTransactionByID(ctx context.Context, id uuid.UUID) (*models.Transaction, error) {
	tx, err := s.txStore.GetByID(ctx, id)
	if err != nil {
		return nil, newStorageError(err, "getting transaction by ID")
	}
	return tx, nil
}

// GetTransactionByExternalID retrieves a transaction by side and external_id from OpenSearch.
func (s *Service) GetTransactionByExternalID(ctx context.Context, side models.TransactionSide, externalID string) (*models.Transaction, error) {
	tx, err := s.txStore.GetByExternalID(ctx, side, externalID)
	if err != nil {
		return nil, newStorageError(err, "getting transaction by external ID")
	}
	return tx, nil
}

// GetTransactionsByProvider returns all transactions for a given provider and side from OpenSearch.
func (s *Service) GetTransactionsByProvider(ctx context.Context, provider string, side models.TransactionSide) ([]models.Transaction, error) {
	transactions, err := s.txStore.GetByProvider(ctx, provider, side)
	if err != nil {
		return nil, newStorageError(err, "getting transactions by provider")
	}
	// Convert []*models.Transaction to []models.Transaction for interface compatibility
	result := make([]models.Transaction, len(transactions))
	for i, tx := range transactions {
		result[i] = *tx
	}
	return result, nil
}

// DeleteTransactionsByProvider is a no-op since transactions are now stored in OpenSearch.
// OpenSearch documents are managed through their TTL or manual index cleanup.
// This method exists for interface compatibility.
func (s *Service) DeleteTransactionsByProvider(ctx context.Context, provider string, side models.TransactionSide) (int64, error) {
	// No-op: transactions in OpenSearch are not deleted through this interface
	// Use OpenSearch index management for cleanup operations
	return 0, nil
}
