package ingestion

import (
	"context"
	"encoding/json"
	"time"
)

const (
	// DegradedLagThreshold is the threshold in seconds after which the ingestion
	// is considered degraded (5 minutes).
	DegradedLagThreshold = 5 * 60 // 5 minutes in seconds

	// HealthStatusHealthy indicates the consumer is running and lag is acceptable.
	HealthStatusHealthy = "healthy"
	// HealthStatusDegraded indicates the consumer is running but lag exceeds threshold.
	HealthStatusDegraded = "degraded"
	// HealthStatusUnhealthy indicates the consumer is down or has errors.
	HealthStatusUnhealthy = "unhealthy"
)

// IngestionHealthStatus represents the health status of an ingestion consumer.
type IngestionHealthStatus struct {
	Status     string  `json:"status"`
	LagSeconds float64 `json:"lag_seconds"`
	Error      string  `json:"error,omitempty"`
}

// IngestionHealthChecker provides health status for the unified ingestion service.
type IngestionHealthChecker struct {
	unifiedService *UnifiedIngestionService
}

// NewIngestionHealthChecker creates a new IngestionHealthChecker.
func NewIngestionHealthChecker(
	unifiedService *UnifiedIngestionService,
) *IngestionHealthChecker {
	return &IngestionHealthChecker{
		unifiedService: unifiedService,
	}
}

// GetStatus returns the health status of the unified ingestion service.
func (h *IngestionHealthChecker) GetStatus() IngestionHealthStatus {
	if h.unifiedService == nil {
		return IngestionHealthStatus{
			Status:     HealthStatusUnhealthy,
			LagSeconds: 0,
			Error:      "unified ingestion service not configured",
		}
	}
	return h.getServiceStatus(h.unifiedService)
}

// ingestionService is an interface for common methods between services.
type ingestionService interface {
	Health() error
	getConsumer() *GenericConsumer
}

func (h *IngestionHealthChecker) getServiceStatus(svc ingestionService) IngestionHealthStatus {
	// Check basic health
	err := svc.Health()
	if err != nil {
		return IngestionHealthStatus{
			Status:     HealthStatusUnhealthy,
			LagSeconds: 0,
			Error:      err.Error(),
		}
	}

	// Get lag from consumer
	consumer := svc.getConsumer()
	if consumer == nil {
		return IngestionHealthStatus{
			Status:     HealthStatusHealthy,
			LagSeconds: 0,
		}
	}

	lagSeconds := consumer.GetLagSeconds()

	// Determine status based on lag
	status := HealthStatusHealthy
	if lagSeconds > DegradedLagThreshold {
		status = HealthStatusDegraded
	}

	return IngestionHealthStatus{
		Status:     status,
		LagSeconds: lagSeconds,
	}
}

// IngestionHealthResponse is the structure returned in the health check response.
type IngestionHealthResponse struct {
	Unified *IngestionHealthStatus `json:"unified,omitempty"`
}

// IngestionHealthCheck implements the health.Check interface for ingestion services.
type IngestionHealthCheck struct {
	checker *IngestionHealthChecker
}

// NewIngestionHealthCheck creates a new health check for ingestion services.
func NewIngestionHealthCheck(checker *IngestionHealthChecker) *IngestionHealthCheck {
	return &IngestionHealthCheck{checker: checker}
}

// Check implements the health.Check interface.
// It returns an error with JSON-encoded status if the service is unhealthy.
func (c *IngestionHealthCheck) Check(ctx context.Context) error {
	status := c.checker.GetStatus()

	// If service is healthy, return nil
	if status.Status == HealthStatusHealthy {
		return nil
	}

	// If service is unhealthy, return an error with details
	if status.Status == HealthStatusUnhealthy {
		return &IngestionHealthError{
			Status: status,
		}
	}

	// For degraded status, we still consider it healthy for the health check
	// but the lag information will be included in the response
	return nil
}

// IngestionHealthError represents a health check error with detailed status.
type IngestionHealthError struct {
	Status IngestionHealthStatus
}

func (e *IngestionHealthError) Error() string {
	data, _ := json.Marshal(map[string]interface{}{
		"ingestion": e.Status,
	})
	return string(data)
}

// ComputeStatus computes the health status based on lag and running state.
func ComputeStatus(isRunning bool, lagSeconds float64, err error) string {
	if err != nil || !isRunning {
		return HealthStatusUnhealthy
	}
	if lagSeconds > DegradedLagThreshold {
		return HealthStatusDegraded
	}
	return HealthStatusHealthy
}

// UnifiedIngestionHealthCheck is a named health check for unified ingestion.
type UnifiedIngestionHealthCheck struct {
	service *UnifiedIngestionService
}

// NewUnifiedIngestionHealthCheck creates a new health check for unified ingestion.
func NewUnifiedIngestionHealthCheck(service *UnifiedIngestionService) *UnifiedIngestionHealthCheck {
	return &UnifiedIngestionHealthCheck{service: service}
}

// Name returns the name of the health check.
func (c *UnifiedIngestionHealthCheck) Name() string {
	return "ingestion.unified"
}

// Check implements the health.Check interface.
func (c *UnifiedIngestionHealthCheck) Check(ctx context.Context) error {
	if c.service == nil {
		return &HealthCheckError{
			Status:     HealthStatusUnhealthy,
			LagSeconds: 0,
			Message:    "unified ingestion service not configured",
		}
	}

	err := c.service.Health()
	consumer := c.service.getConsumer()

	var lagSeconds float64
	if consumer != nil {
		lagSeconds = consumer.GetLagSeconds()
	}

	status := ComputeStatus(err == nil, lagSeconds, err)

	if status == HealthStatusUnhealthy {
		errMsg := ""
		if err != nil {
			errMsg = err.Error()
		}
		return &HealthCheckError{
			Status:     status,
			LagSeconds: lagSeconds,
			Message:    errMsg,
		}
	}

	return nil
}

// HealthCheckError represents an error with health status details.
type HealthCheckError struct {
	Status     string  `json:"status"`
	LagSeconds float64 `json:"lag_seconds"`
	Message    string  `json:"error,omitempty"`
}

func (e *HealthCheckError) Error() string {
	data, _ := json.Marshal(e)
	return string(data)
}

// GetStatus returns a structured IngestionHealthStatus for a given service.
func GetStatus(service interface {
	Health() error
	getConsumer() *GenericConsumer
}) IngestionHealthStatus {
	if service == nil {
		return IngestionHealthStatus{
			Status:     HealthStatusUnhealthy,
			LagSeconds: 0,
			Error:      "service not configured",
		}
	}

	err := service.Health()
	consumer := service.getConsumer()

	var lagSeconds float64
	if consumer != nil {
		lagSeconds = consumer.GetLagSeconds()
	}

	var errMsg string
	if err != nil {
		errMsg = err.Error()
	}

	status := ComputeStatus(err == nil, lagSeconds, err)

	return IngestionHealthStatus{
		Status:     status,
		LagSeconds: lagSeconds,
		Error:      errMsg,
	}
}

// Now returns the current time. It can be overridden in tests.
var Now = time.Now
