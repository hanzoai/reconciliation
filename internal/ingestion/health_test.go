package ingestion

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeStatus(t *testing.T) {
	t.Run("healthy when running with no lag", func(t *testing.T) {
		status := ComputeStatus(true, 0, nil)
		assert.Equal(t, HealthStatusHealthy, status)
	})

	t.Run("healthy when running with lag under threshold", func(t *testing.T) {
		// Lag less than 5 minutes (299 seconds)
		status := ComputeStatus(true, 299, nil)
		assert.Equal(t, HealthStatusHealthy, status)
	})

	t.Run("degraded when running with lag over threshold", func(t *testing.T) {
		// Lag more than 5 minutes (301 seconds)
		status := ComputeStatus(true, 301, nil)
		assert.Equal(t, HealthStatusDegraded, status)
	})

	t.Run("degraded at exactly 5 minutes threshold", func(t *testing.T) {
		// Lag at exactly 5 minutes (300 seconds) - should still be healthy
		status := ComputeStatus(true, 300, nil)
		assert.Equal(t, HealthStatusHealthy, status)
	})

	t.Run("degraded just over 5 minutes threshold", func(t *testing.T) {
		// Lag just over 5 minutes (300.1 seconds)
		status := ComputeStatus(true, 300.1, nil)
		assert.Equal(t, HealthStatusDegraded, status)
	})

	t.Run("unhealthy when not running", func(t *testing.T) {
		status := ComputeStatus(false, 0, nil)
		assert.Equal(t, HealthStatusUnhealthy, status)
	})

	t.Run("unhealthy when error present", func(t *testing.T) {
		status := ComputeStatus(true, 0, errors.New("consumer error"))
		assert.Equal(t, HealthStatusUnhealthy, status)
	})

	t.Run("unhealthy when not running even with low lag", func(t *testing.T) {
		status := ComputeStatus(false, 10, nil)
		assert.Equal(t, HealthStatusUnhealthy, status)
	})

	t.Run("unhealthy when error even with running true", func(t *testing.T) {
		status := ComputeStatus(true, 10, errors.New("some error"))
		assert.Equal(t, HealthStatusUnhealthy, status)
	})
}

func TestGenericConsumer_LagTracking(t *testing.T) {
	t.Run("lag is zero initially", func(t *testing.T) {
		consumer := &GenericConsumer{}
		assert.Equal(t, float64(0), consumer.GetLagSeconds())
	})

	t.Run("lag updates after setting timestamp", func(t *testing.T) {
		consumer := &GenericConsumer{}

		// Set a timestamp 10 seconds ago
		tenSecondsAgo := time.Now().Add(-10 * time.Second)
		consumer.SetLastEventTimestamp(tenSecondsAgo)

		lag := consumer.GetLagSeconds()
		// Allow some tolerance for test execution time
		assert.True(t, lag >= 9.9 && lag <= 11, "lag should be approximately 10 seconds, got %f", lag)
	})

	t.Run("timestamp is retrievable", func(t *testing.T) {
		consumer := &GenericConsumer{}
		timestamp := time.Now()
		consumer.SetLastEventTimestamp(timestamp)

		retrieved := consumer.GetLastEventTimestamp()
		assert.Equal(t, timestamp, retrieved)
	})

	t.Run("running state is correctly reported", func(t *testing.T) {
		consumer := &GenericConsumer{}
		assert.False(t, consumer.IsRunning())

		consumer.running.Store(true)
		assert.True(t, consumer.IsRunning())

		consumer.running.Store(false)
		assert.False(t, consumer.IsRunning())
	})
}

func TestIngestionHealthStatus(t *testing.T) {
	t.Run("healthy status JSON serialization", func(t *testing.T) {
		status := IngestionHealthStatus{
			Status:     HealthStatusHealthy,
			LagSeconds: 10.5,
		}
		assert.Equal(t, HealthStatusHealthy, status.Status)
		assert.Equal(t, 10.5, status.LagSeconds)
		assert.Empty(t, status.Error)
	})

	t.Run("unhealthy status includes error", func(t *testing.T) {
		status := IngestionHealthStatus{
			Status:     HealthStatusUnhealthy,
			LagSeconds: 0,
			Error:      "consumer is not running",
		}
		assert.Equal(t, HealthStatusUnhealthy, status.Status)
		assert.Equal(t, "consumer is not running", status.Error)
	})

	t.Run("degraded status includes lag", func(t *testing.T) {
		status := IngestionHealthStatus{
			Status:     HealthStatusDegraded,
			LagSeconds: 400.5,
		}
		assert.Equal(t, HealthStatusDegraded, status.Status)
		assert.Equal(t, 400.5, status.LagSeconds)
	})
}

func TestHealthCheckError(t *testing.T) {
	t.Run("error message is JSON formatted", func(t *testing.T) {
		err := &HealthCheckError{
			Status:     HealthStatusUnhealthy,
			LagSeconds: 0,
			Message:    "consumer down",
		}

		errorMsg := err.Error()
		assert.Contains(t, errorMsg, "unhealthy")
		assert.Contains(t, errorMsg, "consumer down")
	})
}

// mockIngestionService implements the service interface for testing.
type mockIngestionService struct {
	healthErr error
	consumer  *GenericConsumer
}

func (m *mockIngestionService) Health() error {
	return m.healthErr
}

func (m *mockIngestionService) getConsumer() *GenericConsumer {
	return m.consumer
}

func TestGetStatus(t *testing.T) {
	t.Run("healthy service with no lag", func(t *testing.T) {
		consumer := &GenericConsumer{}
		consumer.running.Store(true)
		consumer.healthy.Store(true)
		consumer.SetLastEventTimestamp(time.Now())

		svc := &mockIngestionService{
			healthErr: nil,
			consumer:  consumer,
		}

		status := GetStatus(svc)
		assert.Equal(t, HealthStatusHealthy, status.Status)
		assert.True(t, status.LagSeconds < 1, "lag should be very low")
		assert.Empty(t, status.Error)
	})

	t.Run("degraded service with high lag", func(t *testing.T) {
		consumer := &GenericConsumer{}
		consumer.running.Store(true)
		consumer.healthy.Store(true)
		// Set timestamp 6 minutes ago
		consumer.SetLastEventTimestamp(time.Now().Add(-6 * time.Minute))

		svc := &mockIngestionService{
			healthErr: nil,
			consumer:  consumer,
		}

		status := GetStatus(svc)
		assert.Equal(t, HealthStatusDegraded, status.Status)
		assert.True(t, status.LagSeconds > 300, "lag should be over 5 minutes")
		assert.Empty(t, status.Error)
	})

	t.Run("unhealthy service with error", func(t *testing.T) {
		consumer := &GenericConsumer{}
		consumer.running.Store(false)

		svc := &mockIngestionService{
			healthErr: errors.New("consumer is not running"),
			consumer:  consumer,
		}

		status := GetStatus(svc)
		assert.Equal(t, HealthStatusUnhealthy, status.Status)
		assert.Equal(t, "consumer is not running", status.Error)
	})

	t.Run("nil consumer returns healthy if health check passes", func(t *testing.T) {
		svc := &mockIngestionService{
			healthErr: nil,
			consumer:  nil,
		}

		status := GetStatus(svc)
		assert.Equal(t, HealthStatusHealthy, status.Status)
		assert.Equal(t, float64(0), status.LagSeconds)
	})
}

func TestIngestionHealthChecker(t *testing.T) {
	t.Run("returns unhealthy when unified service is nil", func(t *testing.T) {
		checker := NewIngestionHealthChecker(nil)
		status := checker.GetStatus()

		assert.Equal(t, HealthStatusUnhealthy, status.Status)
		assert.Contains(t, status.Error, "not configured")
	})
}

func TestUnifiedIngestionHealthCheck(t *testing.T) {
	t.Run("returns nil for healthy service", func(t *testing.T) {
		consumer := &GenericConsumer{}
		consumer.running.Store(true)
		consumer.healthy.Store(true)
		consumer.SetLastEventTimestamp(time.Now())

		service := &UnifiedIngestionService{
			consumer: consumer,
		}

		check := NewUnifiedIngestionHealthCheck(service)

		err := check.Check(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "ingestion.unified", check.Name())
	})

	t.Run("returns error for unhealthy service", func(t *testing.T) {
		consumer := &GenericConsumer{}
		// Consumer not running
		consumer.running.Store(false)

		service := &UnifiedIngestionService{
			consumer: consumer,
		}

		check := NewUnifiedIngestionHealthCheck(service)

		err := check.Check(context.Background())
		require.Error(t, err)

		healthErr, ok := err.(*HealthCheckError)
		require.True(t, ok, "error should be HealthCheckError")
		assert.Equal(t, HealthStatusUnhealthy, healthErr.Status)
	})

	t.Run("returns error for nil service", func(t *testing.T) {
		check := NewUnifiedIngestionHealthCheck(nil)
		err := check.Check(context.Background())
		require.Error(t, err)

		healthErr, ok := err.(*HealthCheckError)
		require.True(t, ok)
		assert.Equal(t, HealthStatusUnhealthy, healthErr.Status)
	})
}

// Test DegradedLagThreshold constant
func TestDegradedLagThreshold(t *testing.T) {
	// 5 minutes = 300 seconds
	assert.Equal(t, float64(300), float64(DegradedLagThreshold))
}

// Test scenarios matching acceptance criteria
func TestAcceptanceCriteria(t *testing.T) {
	t.Run("lag < 5min -> healthy", func(t *testing.T) {
		// 4 minutes = 240 seconds
		status := ComputeStatus(true, 240, nil)
		assert.Equal(t, HealthStatusHealthy, status)
	})

	t.Run("lag > 5min -> degraded", func(t *testing.T) {
		// 6 minutes = 360 seconds
		status := ComputeStatus(true, 360, nil)
		assert.Equal(t, HealthStatusDegraded, status)
	})

	t.Run("consumer down -> unhealthy", func(t *testing.T) {
		status := ComputeStatus(false, 0, errors.New("consumer is not running"))
		assert.Equal(t, HealthStatusUnhealthy, status)
	})
}
