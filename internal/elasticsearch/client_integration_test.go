package elasticsearch_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/health"
	"github.com/formancehq/reconciliation/internal/elasticsearch"
	"github.com/testcontainers/testcontainers-go/modules/opensearch"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
)

func TestOpenSearchIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start OpenSearch container (supports ISM plugin)
	osContainer, err := opensearch.Run(ctx, "opensearchproject/opensearch:2.11.1")
	require.NoError(t, err)
	defer func() { _ = osContainer.Terminate(ctx) }()

	// Get the connection URL
	osURL, err := osContainer.Address(ctx)
	require.NoError(t, err)

	t.Run("successful connection with valid credentials", func(t *testing.T) {
		config := elasticsearch.Config{
			URL:      osURL,
			Username: osContainer.User,
			Password: osContainer.Password,
		}

		client, err := elasticsearch.NewClient(config)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Verify we can ping
		err = client.Ping(ctx)
		assert.NoError(t, err)

		// Verify health check works
		err = client.Health(ctx)
		assert.NoError(t, err)
	})

	t.Run("error with unreachable server", func(t *testing.T) {
		config := elasticsearch.Config{
			URL:      "http://localhost:59999", // Unreachable port
			Username: "admin",
			Password: "password",
		}

		client, err := elasticsearch.NewClient(config)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Ping should fail with unreachable server
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		err = client.Ping(pingCtx)
		assert.Error(t, err)
	})

	t.Run("health check returns healthy if OpenSearch up", func(t *testing.T) {
		config := elasticsearch.Config{
			URL:      osURL,
			Username: osContainer.User,
			Password: osContainer.Password,
		}

		// Create a test fx app with the OpenSearch module
		var healthController *health.HealthController
		ismConfig := elasticsearch.DefaultISMConfig()
		app := fxtest.New(t,
			health.Module(),
			elasticsearch.ModuleWithConfig(config, ismConfig, "test-stack"),
			fx.Populate(&healthController),
		)
		app.RequireStart()
		defer app.RequireStop()

		// Health check should pass when OpenSearch is up
		req := httptest.NewRequest(http.MethodGet, "/_healthcheck", nil)
		rec := httptest.NewRecorder()

		healthController.Check(rec, req)

		// Check that the response is 200 OK
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("health check returns unhealthy if OpenSearch down", func(t *testing.T) {
		// Use an unreachable URL to simulate OpenSearch being down
		config := elasticsearch.Config{
			URL: "http://localhost:59999", // Unreachable port
		}

		client, err := elasticsearch.NewClient(config)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Health check should fail when OpenSearch is unreachable
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		err = client.Health(checkCtx)
		assert.Error(t, err)
	})

	t.Run("module provides client via fx", func(t *testing.T) {
		config := elasticsearch.Config{
			URL:      osURL,
			Username: osContainer.User,
			Password: osContainer.Password,
		}

		var esClient *elasticsearch.Client
		ismConfig := elasticsearch.DefaultISMConfig()
		app := fxtest.New(t,
			health.Module(),
			elasticsearch.ModuleWithConfig(config, ismConfig, "test-stack"),
			fx.Populate(&esClient),
		)
		app.RequireStart()
		defer app.RequireStop()

		require.NotNil(t, esClient)

		// Verify the injected client works
		err := esClient.Ping(ctx)
		assert.NoError(t, err)
	})

	t.Run("ISM policy can be created and retrieved", func(t *testing.T) {
		config := elasticsearch.Config{
			URL:      osURL,
			Username: osContainer.User,
			Password: osContainer.Password,
		}

		client, err := elasticsearch.NewClient(config)
		require.NoError(t, err)

		ismConfig := elasticsearch.DefaultISMConfig()
		stack := "test-ism-stack"

		// Ensure policy doesn't exist initially
		exists, err := client.ISMPolicyExists(ctx, stack)
		require.NoError(t, err)
		assert.False(t, exists)

		// Create the policy
		err = client.CreateISMPolicy(ctx, stack, ismConfig)
		require.NoError(t, err)

		// Verify it exists now
		exists, err = client.ISMPolicyExists(ctx, stack)
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestConfigValidation(t *testing.T) {
	t.Run("valid config with URL only", func(t *testing.T) {
		config := elasticsearch.Config{
			URL: "http://localhost:9200",
		}
		err := config.Validate()
		assert.NoError(t, err)
	})

	t.Run("valid config with credentials", func(t *testing.T) {
		config := elasticsearch.Config{
			URL:      "http://localhost:9200",
			Username: "admin",
			Password: "password",
		}
		err := config.Validate()
		assert.NoError(t, err)
	})

	t.Run("invalid config without URL", func(t *testing.T) {
		config := elasticsearch.Config{}
		err := config.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "URL is required")
	})
}

func TestNewClientValidation(t *testing.T) {
	t.Run("fails with invalid config", func(t *testing.T) {
		config := elasticsearch.Config{}
		_, err := elasticsearch.NewClient(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid opensearch config")
	})
}
