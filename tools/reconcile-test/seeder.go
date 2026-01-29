package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	nc "github.com/nats-io/nats.go"
)

// Publisher wraps a watermill publisher for sending events.
type Publisher struct {
	publisher message.Publisher
}

// NewPublisher creates a new NATS publisher.
func NewPublisher(natsURL string) (*Publisher, error) {
	logger := watermill.NewStdLogger(false, false)
	publisher, err := nats.NewPublisher(
		nats.PublisherConfig{
			URL:         natsURL,
			NatsOptions: []nc.Option{},
			JetStream: nats.JetStreamConfig{
				Disabled: false,
			},
			Marshaler: &nats.NATSMarshaler{},
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS publisher: %w", err)
	}

	return &Publisher{publisher: publisher}, nil
}

// Close closes the publisher.
func (p *Publisher) Close() error {
	return p.publisher.Close()
}

// pendingEvent is an event queued for publishing.
type pendingEvent struct {
	topic string
	data  []byte
}

// publishAll publishes all pending events concurrently using a worker pool.
func (p *Publisher) publishAll(ctx context.Context, events []pendingEvent, concurrency int) error {
	if concurrency < 1 {
		concurrency = 1
	}

	var (
		firstErr atomic.Value
		wg       sync.WaitGroup
		sem      = make(chan struct{}, concurrency)
	)

	for _, ev := range events {
		if ctx.Err() != nil {
			break
		}
		// Check if an error occurred
		if firstErr.Load() != nil {
			break
		}

		wg.Add(1)
		sem <- struct{}{} // acquire slot

		go func(e pendingEvent) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			msg := message.NewMessage(uuid.New().String(), e.data)
			msg.SetContext(ctx)

			if err := p.publisher.Publish(e.topic, msg); err != nil {
				firstErr.CompareAndSwap(nil, fmt.Errorf("failed to publish to %s: %w", e.topic, err))
			}
		}(ev)
	}

	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return v.(error)
	}
	return nil
}

// LedgerEvent represents a ledger event to be published.
type LedgerEvent struct {
	Type    string                 `json:"type"`
	Ledger  string                 `json:"ledger"`
	Payload map[string]interface{} `json:"payload"`
}

// PaymentEvent represents a payment event to be published.
type PaymentEvent struct {
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

// ToJSON converts an event to JSON bytes.
func (e LedgerEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// ToJSON converts an event to JSON bytes.
func (e PaymentEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// SeedConfig holds configuration for seeding.
type SeedConfig struct {
	LedgerTopic    string
	PaymentsTopic  string
	Count          int
	Concurrency    int
	Verbose        bool
	RunPrefix      string          // Unique prefix for this run (to avoid idempotence conflicts)
	ScenarioFilter map[string]bool // If non-nil, only run scenarios in this map
}

// SeedStats tracks what was seeded.
type SeedStats struct {
	LedgerEvents  int
	PaymentEvents int
}

// EventGenerator generates mock events for testing.
type EventGenerator struct {
	rng       *rand.Rand
	runPrefix string // Unique prefix for this run
	configs   map[string]ScenarioConfig
}

// NewEventGenerator creates a new event generator.
func NewEventGenerator() *EventGenerator {
	return &EventGenerator{
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		runPrefix: "", // Will be set by SeedTestData
		configs:   GetScenarioConfigs(),
	}
}

// getLedgerName returns the ledger name for a scenario.
func (g *EventGenerator) getLedgerName(scenario string) string {
	if cfg, ok := g.configs[scenario]; ok {
		return cfg.LedgerName
	}
	return "test-ledger-unknown"
}

// getProviderName returns the connector/provider name for a scenario.
func (g *EventGenerator) getProviderName(scenario string) string {
	if cfg, ok := g.configs[scenario]; ok {
		return cfg.ConnectorType
	}
	return "test-provider-unknown"
}

// makeExternalID creates a unique external ID with the run prefix.
func (g *EventGenerator) makeExternalID(base string) string {
	if g.runPrefix != "" {
		return fmt.Sprintf("%s-%s", g.runPrefix, base)
	}
	return base
}

// generateLedgerEvent creates a ledger event for a specific scenario.
func (g *EventGenerator) generateLedgerEvent(
	scenario string,
	externalID string,
	amount int64,
	currency string,
	timestamp time.Time,
	metadata map[string]interface{},
) LedgerEvent {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}
	metadata["test_scenario"] = scenario

	return LedgerEvent{
		Type:   "COMMITTED_TRANSACTIONS",
		Ledger: g.getLedgerName(scenario),
		Payload: map[string]interface{}{
			"transactions": []map[string]interface{}{
				{
					"id":        externalID,
					"timestamp": timestamp.Format(time.RFC3339),
					"postings": []map[string]interface{}{
						{
							"source":      "world",
							"destination": "users:test",
							"amount":      amount,
							"asset":       currency,
						},
					},
					"metadata": metadata,
				},
			},
		},
	}
}

// generatePaymentEvent creates a payment event for a specific scenario.
func (g *EventGenerator) generatePaymentEvent(
	scenario string,
	externalID string,
	amount int64,
	currency string,
	timestamp time.Time,
	metadata map[string]interface{},
) PaymentEvent {
	if metadata == nil {
		metadata = make(map[string]interface{})
	}
	metadata["test_scenario"] = scenario

	return PaymentEvent{
		Type: "SAVED_PAYMENT",
		Payload: map[string]interface{}{
			"id":        externalID,
			"provider":  g.getProviderName(scenario),
			"amount":    amount,
			"asset":     currency,
			"createdAt": timestamp.Format(time.RFC3339),
			"status":    "SUCCEEDED",
			"metadata":  metadata,
		},
	}
}

// queueLedgerEvent marshals and appends a ledger event to the pending queue.
func queueLedgerEvent(queue *[]pendingEvent, topic string, event LedgerEvent) error {
	data, err := event.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal ledger event: %w", err)
	}
	*queue = append(*queue, pendingEvent{topic: topic, data: data})
	return nil
}

// queuePaymentEvent marshals and appends a payment event to the pending queue.
func queuePaymentEvent(queue *[]pendingEvent, topic string, event PaymentEvent) error {
	data, err := event.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal payment event: %w", err)
	}
	*queue = append(*queue, pendingEvent{topic: topic, data: data})
	return nil
}

// SeedTestData seeds all test scenarios and returns a manifest for verification.
// Events are generated first, then published concurrently via a worker pool.
func SeedTestData(ctx context.Context, publisher *Publisher, config SeedConfig) (*SeedTracker, SeedStats, error) {
	gen := NewEventGenerator()
	tracker := NewSeedTracker()
	stats := SeedStats{}

	// Generate a unique run prefix if not provided
	runPrefix := config.RunPrefix
	if runPrefix == "" {
		runPrefix = fmt.Sprintf("run-%d", time.Now().UnixNano())
	}
	gen.runPrefix = runPrefix

	if config.Verbose {
		fmt.Printf("      Using run prefix: %s\n", runPrefix)
	}

	shouldRun := func(scenario string) bool {
		if config.ScenarioFilter == nil {
			return true
		}
		return config.ScenarioFilter[scenario]
	}

	// Phase 1: Generate all events into a queue
	var queue []pendingEvent

	if shouldRun("perfect-match") {
		if config.Verbose {
			fmt.Println("      Generating perfect match events...")
		}
		if err := generatePerfectMatches(gen, config, tracker, &stats, &queue); err != nil {
			return nil, stats, fmt.Errorf("failed to generate perfect matches: %w", err)
		}
	}

	if shouldRun("probabilistic") {
		if config.Verbose {
			fmt.Println("      Generating probabilistic match events...")
		}
		if err := generateProbabilisticMatches(gen, config, tracker, &stats, &queue); err != nil {
			return nil, stats, fmt.Errorf("failed to generate probabilistic matches: %w", err)
		}
	}

	if shouldRun("orphan-ledger") {
		if config.Verbose {
			fmt.Println("      Generating orphan ledger events...")
		}
		if err := generateOrphanLedger(gen, config, tracker, &stats, &queue); err != nil {
			return nil, stats, fmt.Errorf("failed to generate orphan ledger: %w", err)
		}
	}

	if shouldRun("orphan-payment") {
		if config.Verbose {
			fmt.Println("      Generating orphan payment events...")
		}
		if err := generateOrphanPayments(gen, config, tracker, &stats, &queue); err != nil {
			return nil, stats, fmt.Errorf("failed to generate orphan payments: %w", err)
		}
	}

	if shouldRun("one-to-many") {
		if config.Verbose {
			fmt.Println("      Generating one-to-many events...")
		}
		if err := generateOneToMany(gen, config, tracker, &stats, &queue); err != nil {
			return nil, stats, fmt.Errorf("failed to generate one-to-many: %w", err)
		}
	}

	if shouldRun("many-to-one") {
		if config.Verbose {
			fmt.Println("      Generating many-to-one events...")
		}
		if err := generateManyToOne(gen, config, tracker, &stats, &queue); err != nil {
			return nil, stats, fmt.Errorf("failed to generate many-to-one: %w", err)
		}
	}

	// Phase 2: Publish all events concurrently
	concurrency := config.Concurrency
	if concurrency < 1 {
		concurrency = 50
	}

	fmt.Printf("      Publishing %d events (concurrency: %d)...\n", len(queue), concurrency)
	start := time.Now()

	if err := publisher.publishAll(ctx, queue, concurrency); err != nil {
		return nil, stats, fmt.Errorf("bulk publish failed: %w", err)
	}

	fmt.Printf("      Published %d ledger + %d payment = %d events in %s\n",
		stats.LedgerEvents, stats.PaymentEvents, len(queue), time.Since(start).Round(time.Millisecond))

	return tracker, stats, nil
}

// generatePerfectMatches creates pairs that will match perfectly by external_id.
func generatePerfectMatches(gen *EventGenerator, config SeedConfig, tracker *SeedTracker, stats *SeedStats, queue *[]pendingEvent) error {
	scenario := "perfect-match"
	for i := 0; i < config.Count; i++ {
		externalID := gen.makeExternalID(fmt.Sprintf("perfect-match-%d", i))
		amount := int64((i + 1) * 1000)
		currency := "USD/2"
		now := time.Now().UTC()

		ledgerEvent := gen.generateLedgerEvent(scenario, externalID, amount, currency, now, nil)
		paymentEvent := gen.generatePaymentEvent(scenario, externalID, amount, currency, now, nil)

		if err := queueLedgerEvent(queue, config.LedgerTopic, ledgerEvent); err != nil {
			return err
		}
		stats.LedgerEvents++

		if err := queuePaymentEvent(queue, config.PaymentsTopic, paymentEvent); err != nil {
			return err
		}
		stats.PaymentEvents++

		tracker.PerfectMatches = append(tracker.PerfectMatches, SeededPair{
			LedgerExternalID:   externalID,
			PaymentExternalIDs: []string{externalID},
			PolicyName:         "test-perfect-match-usd",
		})
	}

	return nil
}

// generateProbabilisticMatches creates pairs that should match probabilistically.
func generateProbabilisticMatches(gen *EventGenerator, config SeedConfig, tracker *SeedTracker, stats *SeedStats, queue *[]pendingEvent) error {
	scenario := "probabilistic"
	count := config.Count / 2
	if count < 1 {
		count = 1
	}

	for i := 0; i < count; i++ {
		baseAmount := int64((i + 1) * 5000)
		currency := "EUR/2"
		now := time.Now().UTC()
		orderID := fmt.Sprintf("order-%s", uuid.New().String()[:8])
		ledgerExternalID := gen.makeExternalID(fmt.Sprintf("prob-ledger-%d", i))
		paymentExternalID := gen.makeExternalID(fmt.Sprintf("prob-payment-%d", i))

		ledgerMetadata := map[string]interface{}{
			"order_id": orderID,
			"user_id":  "user-123",
		}
		ledgerEvent := gen.generateLedgerEvent(scenario, ledgerExternalID, baseAmount, currency, now, ledgerMetadata)

		tolerancePercent := 3.0
		amountVariation := float64(baseAmount) * (tolerancePercent / 100) * (gen.rng.Float64()*2 - 1)
		paymentAmount := baseAmount + int64(amountVariation)
		if paymentAmount < 0 {
			paymentAmount = baseAmount
		}

		timeVariation := time.Duration(gen.rng.Intn(2)) * time.Hour
		if gen.rng.Intn(2) == 0 {
			timeVariation = -timeVariation
		}
		paymentTime := now.Add(timeVariation)

		paymentMetadata := map[string]interface{}{
			"order_id": orderID,
			"user_id":  "user-123",
		}
		paymentEvent := gen.generatePaymentEvent(scenario, paymentExternalID, paymentAmount, currency, paymentTime, paymentMetadata)

		if err := queueLedgerEvent(queue, config.LedgerTopic, ledgerEvent); err != nil {
			return err
		}
		stats.LedgerEvents++

		if err := queuePaymentEvent(queue, config.PaymentsTopic, paymentEvent); err != nil {
			return err
		}
		stats.PaymentEvents++

		tracker.ProbabilisticMatches = append(tracker.ProbabilisticMatches, SeededPair{
			LedgerExternalID:   ledgerExternalID,
			PaymentExternalIDs: []string{paymentExternalID},
			PolicyName:         "test-probabilistic-match-eur",
		})
	}

	return nil
}

// generateOrphanLedger creates ledger entries with no matching payments.
func generateOrphanLedger(gen *EventGenerator, config SeedConfig, tracker *SeedTracker, stats *SeedStats, queue *[]pendingEvent) error {
	scenario := "orphan-ledger"
	count := config.Count / 2
	if count < 1 {
		count = 1
	}

	for i := 0; i < count; i++ {
		externalID := gen.makeExternalID(fmt.Sprintf("orphan-ledger-%d", i))
		amount := int64((i + 1) * 750)
		currency := "USD/2"
		timestamp := time.Now().UTC().Add(-25 * time.Hour)

		ledgerEvent := gen.generateLedgerEvent(scenario, externalID, amount, currency, timestamp, nil)

		if err := queueLedgerEvent(queue, config.LedgerTopic, ledgerEvent); err != nil {
			return err
		}
		stats.LedgerEvents++

		tracker.OrphanLedger = append(tracker.OrphanLedger, SeededTransaction{
			ExternalID: externalID,
			Side:       "ledger",
			PolicyName: "test-orphan-ledger-usd",
			Amount:     amount,
			Currency:   currency,
		})
	}

	return nil
}

// generateOrphanPayments creates payment entries with no matching ledger entries.
func generateOrphanPayments(gen *EventGenerator, config SeedConfig, tracker *SeedTracker, stats *SeedStats, queue *[]pendingEvent) error {
	scenario := "orphan-payment"
	count := config.Count / 2
	if count < 1 {
		count = 1
	}

	for i := 0; i < count; i++ {
		externalID := gen.makeExternalID(fmt.Sprintf("orphan-payment-%d", i))
		amount := int64((i + 1) * 850)
		currency := "GBP/2"
		timestamp := time.Now().UTC().Add(-25 * time.Hour)

		paymentEvent := gen.generatePaymentEvent(scenario, externalID, amount, currency, timestamp, nil)

		if err := queuePaymentEvent(queue, config.PaymentsTopic, paymentEvent); err != nil {
			return err
		}
		stats.PaymentEvents++

		tracker.OrphanPayments = append(tracker.OrphanPayments, SeededTransaction{
			ExternalID: externalID,
			Side:       "payment",
			PolicyName: "test-orphan-payment-gbp",
			Amount:     amount,
			Currency:   currency,
		})
	}

	return nil
}

// generateOneToMany creates 1:N scenarios (1 ledger -> N payments).
func generateOneToMany(gen *EventGenerator, config SeedConfig, tracker *SeedTracker, stats *SeedStats, queue *[]pendingEvent) error {
	scenario := "one-to-many"
	count := config.Count / 3
	if count < 1 {
		count = 1
	}

	for i := 0; i < count; i++ {
		externalID := gen.makeExternalID(fmt.Sprintf("1toN-%d", i))
		totalAmount := int64((i + 1) * 10000)
		currency := "USD/2"
		now := time.Now().UTC()
		paymentCount := 3

		ledgerEvent := gen.generateLedgerEvent(scenario, externalID, totalAmount, currency, now, nil)
		if err := queueLedgerEvent(queue, config.LedgerTopic, ledgerEvent); err != nil {
			return err
		}
		stats.LedgerEvents++

		paymentIDs := make([]string, paymentCount)
		remainingAmount := totalAmount
		for j := 0; j < paymentCount; j++ {
			var paymentAmount int64
			if j == paymentCount-1 {
				paymentAmount = remainingAmount
			} else {
				paymentAmount = remainingAmount / int64(paymentCount-j)
				variation := gen.rng.Int63n(paymentAmount/4) - paymentAmount/8
				paymentAmount += variation
				if paymentAmount <= 0 {
					paymentAmount = 1
				}
			}
			remainingAmount -= paymentAmount

			paymentID := fmt.Sprintf("%s-p%d", externalID, j)
			paymentIDs[j] = paymentID

			paymentMetadata := map[string]interface{}{
				"split_index": j,
				"parent_id":   externalID,
			}

			paymentEvent := gen.generatePaymentEvent(
				scenario, paymentID, paymentAmount, currency,
				now.Add(time.Duration(j)*time.Minute), paymentMetadata,
			)

			if err := queuePaymentEvent(queue, config.PaymentsTopic, paymentEvent); err != nil {
				return err
			}
			stats.PaymentEvents++
		}

		tracker.OneToManyMatches = append(tracker.OneToManyMatches, SeededPair{
			LedgerExternalID:   externalID,
			PaymentExternalIDs: paymentIDs,
			PolicyName:         "test-one-to-many-usd",
		})
	}

	return nil
}

// generateManyToOne creates N:1 scenarios (N ledgers -> 1 payment).
func generateManyToOne(gen *EventGenerator, config SeedConfig, tracker *SeedTracker, stats *SeedStats, queue *[]pendingEvent) error {
	scenario := "many-to-one"
	count := config.Count / 3
	if count < 1 {
		count = 1
	}

	for i := 0; i < count; i++ {
		externalID := gen.makeExternalID(fmt.Sprintf("Nto1-%d", i))
		totalAmount := int64((i + 1) * 12000)
		currency := "EUR/2"
		now := time.Now().UTC()
		ledgerCount := 3

		paymentEvent := gen.generatePaymentEvent(scenario, externalID, totalAmount, currency, now, nil)
		if err := queuePaymentEvent(queue, config.PaymentsTopic, paymentEvent); err != nil {
			return err
		}
		stats.PaymentEvents++

		ledgerIDs := make([]string, ledgerCount)
		remainingAmount := totalAmount
		for j := 0; j < ledgerCount; j++ {
			var ledgerAmount int64
			if j == ledgerCount-1 {
				ledgerAmount = remainingAmount
			} else {
				ledgerAmount = remainingAmount / int64(ledgerCount-j)
				variation := gen.rng.Int63n(ledgerAmount/4) - ledgerAmount/8
				ledgerAmount += variation
				if ledgerAmount <= 0 {
					ledgerAmount = 1
				}
			}
			remainingAmount -= ledgerAmount

			ledgerID := fmt.Sprintf("%s-l%d", externalID, j)
			ledgerIDs[j] = ledgerID

			ledgerMetadata := map[string]interface{}{
				"split_index": j,
				"parent_id":   externalID,
			}

			ledgerEvent := gen.generateLedgerEvent(
				scenario, ledgerID, ledgerAmount, currency,
				now.Add(time.Duration(j)*time.Minute), ledgerMetadata,
			)

			if err := queueLedgerEvent(queue, config.LedgerTopic, ledgerEvent); err != nil {
				return err
			}
			stats.LedgerEvents++
		}

		tracker.ManyToOneMatches = append(tracker.ManyToOneMatches, SeededPair{
			LedgerExternalID:   ledgerIDs[0],
			PaymentExternalIDs: []string{externalID},
			PolicyName:         "test-many-to-one-eur",
		})
	}

	return nil
}
