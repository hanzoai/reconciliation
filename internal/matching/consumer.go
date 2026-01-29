package matching

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/events"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/formancehq/reconciliation/internal/storage"
	"github.com/google/uuid"
)

// DefaultMatchingWorkers is the default number of matching workers.
const DefaultMatchingWorkers = 10

// DefaultConsumerWorkers is the default number of concurrent message handlers.
const DefaultConsumerWorkers = 10

// PolicyResolver resolves a policy from a transaction's provider and side.
type PolicyResolver interface {
	// ResolvePolicy finds the matching policy for a given provider and side.
	// Returns nil, nil if no policy is found (transaction should be skipped).
	ResolvePolicy(ctx context.Context, provider string, side models.TransactionSide) (*models.Policy, error)
}

// OrchestratorProvider returns a MatchingOrchestrator for a given policy.
type OrchestratorProvider interface {
	GetOrCreate(policy *models.Policy) *MatchingOrchestrator
}

// MatchingConsumer consumes TRANSACTION_INGESTED events from a NATS topic
// and triggers matching via the MatchingOrchestrator.
// Messages are processed concurrently using numWorkers goroutines.
type MatchingConsumer struct {
	subscriber           message.Subscriber
	topic                string
	txStore              storage.TransactionStore
	policyResolver       PolicyResolver
	orchestratorProvider OrchestratorProvider
	numWorkers           int
}

// NewMatchingConsumer creates a new MatchingConsumer.
// numWorkers controls how many messages are processed concurrently (default 10).
func NewMatchingConsumer(
	subscriber message.Subscriber,
	topic string,
	txStore storage.TransactionStore,
	policyResolver PolicyResolver,
	orchestratorProvider OrchestratorProvider,
	numWorkers int,
) *MatchingConsumer {
	if numWorkers <= 0 {
		numWorkers = DefaultConsumerWorkers
	}
	return &MatchingConsumer{
		subscriber:           subscriber,
		topic:                topic,
		txStore:              txStore,
		policyResolver:       policyResolver,
		orchestratorProvider: orchestratorProvider,
		numWorkers:           numWorkers,
	}
}

// Start begins consuming messages from the topic. Blocks until the context is cancelled.
// Messages are dispatched to a pool of numWorkers goroutines for concurrent processing.
func (c *MatchingConsumer) Start(ctx context.Context) error {
	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"topic":      c.topic,
		"numWorkers": c.numWorkers,
	})
	logger.Infof("Matching consumer started with %d workers", c.numWorkers)

	messages, err := c.subscriber.Subscribe(ctx, c.topic)
	if err != nil {
		return err
	}

	sem := make(chan struct{}, c.numWorkers)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			logger.Info("Matching consumer stopping, waiting for in-flight messages...")
			wg.Wait()
			logger.Info("Matching consumer stopped")
			return nil
		case msg, ok := <-messages:
			if !ok {
				logger.Info("Matching consumer channel closed, waiting for in-flight messages...")
				wg.Wait()
				logger.Info("Matching consumer stopped")
				return nil
			}

			sem <- struct{}{} // acquire worker slot
			wg.Add(1)
			go func(m *message.Message) {
				defer wg.Done()
				defer func() { <-sem }() // release worker slot
				c.handleMessage(ctx, m)
			}(msg)
		}
	}
}

// handleMessage processes a single TRANSACTION_INGESTED event.
func (c *MatchingConsumer) handleMessage(ctx context.Context, msg *message.Message) {
	logger := logging.FromContext(ctx)

	// 1. Parse the event envelope
	var event events.Event
	if err := json.Unmarshal(msg.Payload, &event); err != nil {
		logger.WithFields(map[string]interface{}{
			"error":      err.Error(),
			"message_id": msg.UUID,
		}).Debug("Failed to unmarshal event, skipping")
		msg.Ack()
		return
	}

	// Only process TRANSACTION_INGESTED events
	if event.Type != events.EventTypeTransactionIngested {
		logger.WithFields(map[string]interface{}{
			"message_id": msg.UUID,
			"event_type": event.Type,
		}).Debug("Ignoring non-TRANSACTION_INGESTED event")
		msg.Ack()
		return
	}

	// 2. Parse the payload
	payload, err := c.parsePayload(event.Payload)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error":      err.Error(),
			"message_id": msg.UUID,
		}).Error("Failed to parse TRANSACTION_INGESTED payload, skipping")
		msg.Ack()
		return
	}

	logger.WithFields(map[string]interface{}{
		"message_id":    msg.UUID,
		"transactionId": payload.TransactionID,
		"externalId":    payload.ExternalID,
		"side":          payload.Side,
		"provider":      payload.Provider,
	}).Debug("Processing TRANSACTION_INGESTED event")

	// 3. Fetch the full transaction from OpenSearch
	txID, err := uuid.Parse(payload.TransactionID)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error":         err.Error(),
			"transactionId": payload.TransactionID,
		}).Error("Invalid transaction ID, skipping")
		msg.Ack()
		return
	}

	tx, err := c.txStore.GetByID(ctx, txID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			logger.WithFields(map[string]interface{}{
				"transactionId": payload.TransactionID,
			}).Info("Transaction not found in store, skipping")
			msg.Ack()
			return
		}
		logger.WithFields(map[string]interface{}{
			"error":         err.Error(),
			"transactionId": payload.TransactionID,
		}).Error("Failed to fetch transaction from store, nacking for retry")
		msg.Nack()
		return
	}

	// 4. Resolve the policy for this transaction
	policy, err := c.policyResolver.ResolvePolicy(ctx, tx.Provider, tx.Side)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error":    err.Error(),
			"provider": tx.Provider,
			"side":     string(tx.Side),
		}).Error("Failed to resolve policy, nacking for retry")
		msg.Nack()
		return
	}

	if policy == nil {
		logger.WithFields(map[string]interface{}{
			"provider": tx.Provider,
			"side":     string(tx.Side),
		}).Debug("No policy found for transaction, skipping")
		msg.Ack()
		return
	}

	// 5. Get or create the orchestrator for this policy
	orchestrator := c.orchestratorProvider.GetOrCreate(policy)

	// 6. Run matching
	result, err := orchestrator.Match(ctx, tx)
	if err != nil {
		logger.WithFields(map[string]interface{}{
			"error":         err.Error(),
			"transactionId": tx.ID.String(),
			"policyId":      policy.ID.String(),
		}).Error("Matching failed, nacking for retry")
		msg.Nack()
		return
	}

	logger.WithFields(map[string]interface{}{
		"transactionId": tx.ID.String(),
		"policyId":      policy.ID.String(),
		"decision":      string(result.Decision),
		"hasMatch":      result.Match != nil,
		"candidates":    len(result.Candidates),
	}).Info("Matching completed")

	msg.Ack()
}

// parsePayload extracts the TransactionIngestedPayload from the event payload.
func (c *MatchingConsumer) parsePayload(payload interface{}) (*events.TransactionIngestedPayload, error) {
	// The payload comes as map[string]interface{} from JSON unmarshaling
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	var p events.TransactionIngestedPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	if p.TransactionID == "" {
		return nil, fmt.Errorf("missing transactionId in payload")
	}

	return &p, nil
}
