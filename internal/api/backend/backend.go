package backend

import (
	"context"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"

	"github.com/formancehq/reconciliation/internal/api/service"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
)

//go:generate mockgen -source backend.go -destination backend_generated.go -package backend . Service
type Service interface {
	Reconciliation(ctx context.Context, policyID string, req *service.ReconciliationRequest) (*models.Reconciliation, error)
	GetReconciliation(ctx context.Context, id string) (*models.Reconciliation, error)
	ListReconciliations(ctx context.Context, q storage.GetReconciliationsQuery) (*bunpaginate.Cursor[models.Reconciliation], error)

	CreatePolicy(ctx context.Context, req *service.CreatePolicyRequest) (*models.Policy, error)
	DeletePolicy(ctx context.Context, id string) error
	GetPolicy(ctx context.Context, id string) (*models.Policy, error)
	UpdatePolicy(ctx context.Context, id string, req *service.UpdatePolicyRequest) (*models.Policy, error)
	ListPolicies(ctx context.Context, q storage.GetPoliciesQuery) (*bunpaginate.Cursor[models.Policy], error)

	// V2 API methods
	CreatePolicyV2Balance(ctx context.Context, req *service.CreatePolicyV2BalanceRequest) (*models.Policy, error)
	CreatePolicyV2Transaction(ctx context.Context, req *service.CreatePolicyV2TransactionRequest) (*models.Policy, error)
	UpdatePolicyV2(ctx context.Context, id string, req *service.UpdatePolicyV2Request) (*models.Policy, error)

	ForceMatch(ctx context.Context, policyID string, req *service.ForceMatchRequest, resolvedBy string) (*models.Match, error)
	MatchTransactions(ctx context.Context, policyID string, transactionIDs []string) ([]service.MatchResultV2, error)
	TriggerPolicyMatching(ctx context.Context, policyID string) ([]service.MatchResultV2, error)

	ListAnomalies(ctx context.Context, q storage.GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error)
	ListPolicyAnomalies(ctx context.Context, policyID string, q storage.GetAnomaliesQuery) (*bunpaginate.Cursor[models.Anomaly], error)
	GetAnomalyByID(ctx context.Context, anomalyID string) (*service.AnomalyDetailResult, error)

	GetLatestPolicyReport(ctx context.Context, policyID string) (*models.Report, error)

	// Backfill methods
	CreateBackfill(ctx context.Context, req *service.CreateBackfillRequest) (*models.Backfill, error)
	GetBackfill(ctx context.Context, id string) (*models.Backfill, error)

	// Admin/cleanup methods
	DeleteTransactionsByProvider(ctx context.Context, provider string, side models.TransactionSide) (int64, error)
	GetTransactionsByProvider(ctx context.Context, provider string, side models.TransactionSide) ([]models.Transaction, error)
}

type Backend interface {
	GetService() Service
}

type DefaultBackend struct {
	service Service
}

func (d DefaultBackend) GetService() Service {
	return d.service
}

func NewDefaultBackend(service Service) Backend {
	return &DefaultBackend{
		service: service,
	}
}
