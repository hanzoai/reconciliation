package matching

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/formancehq/reconciliation/internal/elasticsearch"
	"github.com/formancehq/reconciliation/internal/models"
	"github.com/google/uuid"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
)

// ProbabilisticMatcherConfig holds configuration for the ProbabilisticMatcher.
type ProbabilisticMatcherConfig struct {
	// MaxCandidates is the maximum number of candidates to return.
	MaxCandidates int
}

// DefaultProbabilisticMatcherConfig returns the default configuration.
func DefaultProbabilisticMatcherConfig() ProbabilisticMatcherConfig {
	return ProbabilisticMatcherConfig{
		MaxCandidates: 10,
	}
}

// ProbabilisticMatcher implements matching based on multi-criteria scoring.
// It uses OpenSearch to search for candidates and scores them based on
// amount similarity, date proximity, and metadata matching.
type ProbabilisticMatcher struct {
	esClient *elasticsearch.Client
	policy   *models.Policy
	stack    string
	config   ProbabilisticMatcherConfig
}

// NewProbabilisticMatcher creates a new ProbabilisticMatcher instance.
func NewProbabilisticMatcher(
	esClient *elasticsearch.Client,
	policy *models.Policy,
	stack string,
	config ProbabilisticMatcherConfig,
) *ProbabilisticMatcher {
	if config.MaxCandidates <= 0 {
		config.MaxCandidates = DefaultProbabilisticMatcherConfig().MaxCandidates
	}
	return &ProbabilisticMatcher{
		esClient: esClient,
		policy:   policy,
		stack:    stack,
		config:   config,
	}
}

// Match attempts to find probabilistic matches for the given transaction.
// It searches for candidates on the opposite side within a configured time window,
// scores them based on amount similarity, date proximity, and metadata,
// and returns the top N candidates.
func (m *ProbabilisticMatcher) Match(ctx context.Context, transaction *models.Transaction) (*MatchResult, error) {
	if transaction == nil {
		return nil, errors.New("transaction cannot be nil")
	}

	// Get scoring configuration with defaults
	scoringConfig := m.getScoringConfig()

	// Search for candidates in OpenSearch
	candidates, err := m.searchCandidates(ctx, transaction, scoringConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to search candidates: %w", err)
	}

	if len(candidates) == 0 {
		return &MatchResult{
			Match:      nil,
			Candidates: []Candidate{},
			Decision:   DecisionUnmatched,
		}, nil
	}

	// Sort candidates by score (descending)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	// Limit to top N candidates
	if len(candidates) > m.config.MaxCandidates {
		candidates = candidates[:m.config.MaxCandidates]
	}

	// Determine decision based on thresholds
	return m.buildResult(transaction, candidates, scoringConfig)
}

// searchCandidates searches for candidate transactions in OpenSearch.
func (m *ProbabilisticMatcher) searchCandidates(
	ctx context.Context,
	transaction *models.Transaction,
	scoringConfig *models.ScoringConfig,
) ([]Candidate, error) {
	// Build the search query
	query := m.buildSearchQuery(transaction, scoringConfig)

	// Serialize the query to JSON
	queryBytes, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search query: %w", err)
	}

	// Search using the stack's index pattern
	indexPattern := elasticsearch.TransactionIndexPattern(m.stack)

	// Execute search using OpenSearch client
	res, err := m.esClient.Client().Search(ctx, &opensearchapi.SearchReq{
		Indices: []string{indexPattern},
		Body:    bytes.NewReader(queryBytes),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute search: %w", err)
	}

	// Convert search hits to candidates with scores
	return m.convertToCandidates(res.Hits.Hits, transaction, scoringConfig)
}

// buildSearchQuery builds the OpenSearch query for candidate search.
func (m *ProbabilisticMatcher) buildSearchQuery(
	transaction *models.Transaction,
	scoringConfig *models.ScoringConfig,
) map[string]interface{} {
	// Determine the opposite side
	oppositeSide := getOppositeSide(transaction.Side)

	// Calculate time window bounds
	timeWindowHours := scoringConfig.TimeWindowHours
	if timeWindowHours <= 0 {
		timeWindowHours = 24 // Default to 24 hours
	}
	timeWindow := time.Duration(timeWindowHours) * time.Hour
	minTime := transaction.OccurredAt.Add(-timeWindow)
	maxTime := transaction.OccurredAt.Add(timeWindow)

	// Build the bool query with must filters
	// Note: policy_id is no longer stored in ES - filtering by policy happens at the DB level
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					// Filter by opposite side
					{
						"term": map[string]interface{}{
							"side": string(oppositeSide),
						},
					},
					// Filter by time window
					{
						"range": map[string]interface{}{
							"occurred_at": map[string]interface{}{
								"gte": minTime.Format(time.RFC3339),
								"lte": maxTime.Format(time.RFC3339),
							},
						},
					},
					// Filter by same currency
					{
						"term": map[string]interface{}{
							"currency": transaction.Currency,
						},
					},
				},
			},
		},
		"size": m.config.MaxCandidates * 2, // Fetch more to allow for post-scoring filtering
	}

	return query
}

// convertToCandidates converts OpenSearch search hits to scored candidates.
func (m *ProbabilisticMatcher) convertToCandidates(
	hits []opensearchapi.SearchHit,
	sourceTransaction *models.Transaction,
	scoringConfig *models.ScoringConfig,
) ([]Candidate, error) {
	candidates := make([]Candidate, 0, len(hits))

	for _, hit := range hits {
		// Convert document back to Transaction model
		tx, err := m.hitToTransaction(hit)
		if err != nil {
			continue // Skip invalid documents
		}

		// Calculate the score
		score, reasons := m.calculateScore(sourceTransaction, tx, scoringConfig)

		candidates = append(candidates, Candidate{
			Transaction: tx,
			Score:       score,
			Reasons:     reasons,
		})
	}

	return candidates, nil
}

// hitToTransaction converts an OpenSearch search hit to a models.Transaction.
func (m *ProbabilisticMatcher) hitToTransaction(hit opensearchapi.SearchHit) (*models.Transaction, error) {
	// Parse the source into TransactionDocument
	var doc elasticsearch.TransactionDocument
	sourceBytes, err := json.Marshal(hit.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source: %w", err)
	}
	if err := json.Unmarshal(sourceBytes, &doc); err != nil {
		return nil, fmt.Errorf("failed to unmarshal source: %w", err)
	}

	return m.documentToTransaction(doc)
}

// documentToTransaction converts a TransactionDocument back to a models.Transaction.
// Note: PolicyID is not stored in ES - transactions retrieved from ES won't have PolicyID set.
func (m *ProbabilisticMatcher) documentToTransaction(doc elasticsearch.TransactionDocument) (*models.Transaction, error) {
	txID, err := uuid.Parse(doc.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction_id: %w", err)
	}

	return &models.Transaction{
		ID:         txID,
		ExternalID: doc.ExternalID,
		Side:       models.TransactionSide(doc.Side),
		Provider:   doc.Provider,
		Amount:     doc.Amount,
		Currency:   doc.Currency,
		OccurredAt: doc.OccurredAt,
		Metadata:   doc.Metadata,
	}, nil
}

// calculateScore calculates a weighted score for a candidate transaction.
func (m *ProbabilisticMatcher) calculateScore(
	source *models.Transaction,
	candidate *models.Transaction,
	scoringConfig *models.ScoringConfig,
) (float64, []string) {
	var reasons []string
	var totalScore float64
	var totalWeight float64

	// Get weights with defaults
	weights := m.getWeights(scoringConfig)

	// Calculate amount score
	if weights.Amount > 0 {
		amountScore := m.calculateAmountScore(source, candidate, scoringConfig)
		totalScore += amountScore * weights.Amount
		totalWeight += weights.Amount
		reasons = append(reasons, fmt.Sprintf("amount similarity: %.2f", amountScore))
	}

	// Calculate date proximity score
	if weights.Date > 0 {
		dateScore := m.calculateDateScore(source, candidate, scoringConfig)
		totalScore += dateScore * weights.Date
		totalWeight += weights.Date
		reasons = append(reasons, fmt.Sprintf("date proximity: %.2f", dateScore))
	}

	// Calculate metadata score
	if weights.Metadata > 0 && source.Metadata != nil && candidate.Metadata != nil {
		metadataScore := m.calculateMetadataScore(source, candidate)
		totalScore += metadataScore * weights.Metadata
		totalWeight += weights.Metadata
		if metadataScore > 0 {
			reasons = append(reasons, fmt.Sprintf("metadata match: %.2f", metadataScore))
		}
	}

	// Normalize to 0-1 range
	if totalWeight > 0 {
		totalScore = totalScore / totalWeight
	}

	return totalScore, reasons
}

// calculateAmountScore calculates a score based on amount similarity.
// Returns 1.0 for exact match, scales down based on tolerance.
func (m *ProbabilisticMatcher) calculateAmountScore(
	source *models.Transaction,
	candidate *models.Transaction,
	scoringConfig *models.ScoringConfig,
) float64 {
	if source.Amount == candidate.Amount {
		return 1.0
	}

	// Calculate percentage difference
	diff := math.Abs(float64(source.Amount - candidate.Amount))
	avgAmount := float64(source.Amount+candidate.Amount) / 2.0
	if avgAmount == 0 {
		return 0
	}

	percentDiff := (diff / avgAmount) * 100

	// Get tolerance
	tolerance := scoringConfig.AmountTolerancePercent
	if tolerance <= 0 {
		tolerance = 1.0 // Default 1% tolerance
	}

	// If within tolerance, scale score from 1.0 to 0.5
	if percentDiff <= tolerance {
		return 1.0 - (percentDiff/tolerance)*0.5
	}

	// Beyond tolerance, scale down more aggressively
	// At 2x tolerance, score is 0.25; at 3x tolerance, score is ~0.1
	return math.Max(0, 0.5*math.Exp(-(percentDiff-tolerance)/tolerance))
}

// calculateDateScore calculates a score based on date proximity.
// Returns 1.0 for same time, uses Gaussian decay based on time window.
func (m *ProbabilisticMatcher) calculateDateScore(
	source *models.Transaction,
	candidate *models.Transaction,
	scoringConfig *models.ScoringConfig,
) float64 {
	timeWindowHours := scoringConfig.TimeWindowHours
	if timeWindowHours <= 0 {
		timeWindowHours = 24
	}

	return ScoreTimeDelta(source.OccurredAt, candidate.OccurredAt, timeWindowHours)
}

// calculateMetadataScore calculates a score based on metadata field matching.
// Returns 1.0 if all fields match, scales down based on match percentage.
func (m *ProbabilisticMatcher) calculateMetadataScore(
	source *models.Transaction,
	candidate *models.Transaction,
) float64 {
	if len(source.Metadata) == 0 {
		return 0
	}

	matchCount := 0
	totalFields := 0

	for key, sourceVal := range source.Metadata {
		totalFields++
		if candidateVal, ok := candidate.Metadata[key]; ok {
			if fmt.Sprintf("%v", sourceVal) == fmt.Sprintf("%v", candidateVal) {
				matchCount++
			}
		}
	}

	if totalFields == 0 {
		return 0
	}

	return float64(matchCount) / float64(totalFields)
}

// getScoringConfig returns the policy's scoring config with defaults.
func (m *ProbabilisticMatcher) getScoringConfig() *models.ScoringConfig {
	if m.policy.ScoringConfig != nil {
		return m.policy.ScoringConfig
	}
	return &models.ScoringConfig{
		TimeWindowHours:        24,
		AmountTolerancePercent: 1.0,
		Weights: &models.ScoringWeights{
			Amount:   0.5,
			Date:     0.3,
			Metadata: 0.2,
		},
		Thresholds: &models.ScoringThresholds{
			AutoMatch: 0.9,
			Review:    0.7,
		},
	}
}

// getWeights returns the scoring weights with defaults.
func (m *ProbabilisticMatcher) getWeights(config *models.ScoringConfig) models.ScoringWeights {
	if config != nil && config.Weights != nil {
		return *config.Weights
	}
	return models.ScoringWeights{
		Amount:   0.5,
		Date:     0.3,
		Metadata: 0.2,
	}
}

// getThresholds returns the scoring thresholds with defaults.
func (m *ProbabilisticMatcher) getThresholds(config *models.ScoringConfig) models.ScoringThresholds {
	if config != nil && config.Thresholds != nil {
		return *config.Thresholds
	}
	return models.ScoringThresholds{
		AutoMatch: 0.9,
		Review:    0.7,
	}
}

// buildResult constructs the MatchResult based on candidates and thresholds.
func (m *ProbabilisticMatcher) buildResult(
	sourceTx *models.Transaction,
	candidates []Candidate,
	scoringConfig *models.ScoringConfig,
) (*MatchResult, error) {
	if len(candidates) == 0 {
		return &MatchResult{
			Match:      nil,
			Candidates: []Candidate{},
			Decision:   DecisionUnmatched,
		}, nil
	}

	thresholds := m.getThresholds(scoringConfig)
	topCandidate := candidates[0]

	// Check thresholds
	if topCandidate.Score >= thresholds.AutoMatch {
		// Auto-match: create match and return MATCHED decision
		match := m.createMatch(sourceTx, topCandidate)
		return &MatchResult{
			Match:      match,
			Candidates: candidates,
			Decision:   DecisionMatched,
		}, nil
	}

	if topCandidate.Score >= thresholds.Review {
		// Requires review: return candidates without match
		return &MatchResult{
			Match:      nil,
			Candidates: candidates,
			Decision:   DecisionRequiresReview,
		}, nil
	}

	// Below thresholds
	return &MatchResult{
		Match:      nil,
		Candidates: candidates,
		Decision:   DecisionUnmatched,
	}, nil
}

// createMatch creates a Match object from a source transaction and candidate.
func (m *ProbabilisticMatcher) createMatch(
	sourceTx *models.Transaction,
	candidate Candidate,
) *models.Match {
	var ledgerTxIDs, paymentTxIDs []uuid.UUID

	if sourceTx.Side == models.TransactionSideLedger {
		ledgerTxIDs = []uuid.UUID{sourceTx.ID}
		paymentTxIDs = []uuid.UUID{candidate.Transaction.ID}
	} else {
		ledgerTxIDs = []uuid.UUID{candidate.Transaction.ID}
		paymentTxIDs = []uuid.UUID{sourceTx.ID}
	}

	return &models.Match{
		ID:                     uuid.New(),
		PolicyID:               m.policy.ID,
		LedgerTransactionIDs:   ledgerTxIDs,
		PaymentsTransactionIDs: paymentTxIDs,
		Score:                  candidate.Score,
		Decision:               models.DecisionMatched,
		Explanation: models.Explanation{
			Reason: "Probabilistic match found",
			Score:  candidate.Score,
			FieldNotes: map[string]string{
				"match_type": "probabilistic",
				"reasons":    fmt.Sprintf("%v", candidate.Reasons),
			},
		},
	}
}

// GetSearchQuery returns the search query for testing purposes.
// This allows unit tests to verify the query structure without needing a real OpenSearch instance.
func (m *ProbabilisticMatcher) GetSearchQuery(
	transaction *models.Transaction,
) map[string]interface{} {
	return m.buildSearchQuery(transaction, m.getScoringConfig())
}
