package elasticsearch

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"

	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// Config holds the OpenSearch client configuration.
type Config struct {
	// URL is the OpenSearch server URL.
	URL string
	// Username is the optional OpenSearch username for authentication.
	Username string
	// Password is the optional OpenSearch password for authentication.
	Password string
	// CACert is the optional CA certificate for TLS verification.
	// If provided, it will be used to verify the server's certificate.
	CACert []byte
	// SkipTLSVerify skips TLS certificate verification.
	// Use with caution, only for testing purposes.
	SkipTLSVerify bool
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.URL == "" {
		return errors.New("opensearch URL is required")
	}
	return nil
}

// Client wraps the OpenSearch client.
type Client struct {
	client *opensearchapi.Client
	config Config
}

// NewClient creates a new OpenSearch client with the given configuration.
func NewClient(config Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid opensearch config: %w", err)
	}

	osConfig := opensearch.Config{
		Addresses: []string{config.URL},
	}

	if config.Username != "" && config.Password != "" {
		osConfig.Username = config.Username
		osConfig.Password = config.Password
	}

	// Configure TLS if needed
	if len(config.CACert) > 0 || config.SkipTLSVerify {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: config.SkipTLSVerify, //nolint:gosec // Configurable for testing
		}

		if len(config.CACert) > 0 {
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(config.CACert) {
				return nil, errors.New("failed to append CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		osConfig.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	// Create the opensearchapi.Client using the opensearchapi.Config wrapper
	client, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: osConfig,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create opensearch client: %w", err)
	}

	return &Client{
		client: client,
		config: config,
	}, nil
}

// Client returns the underlying OpenSearch API client.
func (c *Client) Client() *opensearchapi.Client {
	return c.client
}

// Ping checks if the OpenSearch cluster is reachable.
func (c *Client) Ping(ctx context.Context) error {
	res, err := c.client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to ping opensearch: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode >= 400 {
		return fmt.Errorf("opensearch ping returned error: %d", res.StatusCode)
	}

	return nil
}

// Health checks the cluster health status.
func (c *Client) Health(ctx context.Context) error {
	res, err := c.client.Cluster.Health(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get opensearch cluster health: %w", err)
	}
	defer func() { _ = res.Inspect().Response.Body.Close() }()

	if res.Status == "red" {
		return fmt.Errorf("opensearch cluster health is red")
	}

	return nil
}

// DeleteIndex deletes an index by name.
// Returns (true, nil) if the index was deleted, (false, nil) if it didn't exist.
func (c *Client) DeleteIndex(ctx context.Context, indexName string) (bool, error) {
	res, err := c.client.Indices.Delete(ctx, opensearchapi.IndicesDeleteReq{
		Indices: []string{indexName},
	})
	if err != nil {
		return false, fmt.Errorf("failed to delete index: %w", err)
	}
	defer func() { _ = res.Inspect().Response.Body.Close() }()

	if res.Inspect().Response.StatusCode == http.StatusNotFound {
		return false, nil
	}

	return true, nil
}
