package matching

import (
	"context"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/reconciliation/internal/models"
)

// MatchingTask represents a transaction to be matched asynchronously.
type MatchingTask struct {
	Transaction *models.Transaction
}

// MatchingWorkerPool manages a pool of workers for asynchronous matching.
// It receives transactions via a channel and processes them concurrently
// using the configured number of workers.
type MatchingWorkerPool struct {
	orchestrator *MatchingOrchestrator
	numWorkers   int
	taskChan     chan MatchingTask
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	started      bool
	mu           sync.Mutex
}

// MatchingWorkerPoolConfig holds configuration for the MatchingWorkerPool.
type MatchingWorkerPoolConfig struct {
	// NumWorkers is the number of concurrent workers in the pool.
	// Defaults to 10 if not specified or <= 0.
	NumWorkers int
}

// DefaultMatchingWorkers is the default number of matching workers.
const DefaultMatchingWorkers = 10

// NewMatchingWorkerPool creates a new MatchingWorkerPool.
func NewMatchingWorkerPool(
	orchestrator *MatchingOrchestrator,
	config MatchingWorkerPoolConfig,
) *MatchingWorkerPool {
	numWorkers := config.NumWorkers
	if numWorkers <= 0 {
		numWorkers = DefaultMatchingWorkers
	}

	return &MatchingWorkerPool{
		orchestrator: orchestrator,
		numWorkers:   numWorkers,
		taskChan:     make(chan MatchingTask, numWorkers*2), // Buffer to prevent blocking
	}
}

// Start begins the worker pool. It launches the configured number of workers
// that listen for matching tasks.
func (p *MatchingWorkerPool) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return
	}

	p.ctx, p.cancel = context.WithCancel(ctx)
	p.started = true

	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	logging.FromContext(ctx).Infof("Matching worker pool started with %d workers", p.numWorkers)
}

// Stop gracefully stops the worker pool.
// It closes the task channel and waits for all workers to finish.
func (p *MatchingWorkerPool) Stop() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	p.mu.Unlock()

	if p.cancel != nil {
		p.cancel()
	}
	close(p.taskChan)
	p.wg.Wait()
}

// Submit submits a transaction for asynchronous matching.
// It returns immediately and does not block the caller.
// If the worker pool is stopped or the channel is full, the task is dropped with a log warning.
func (p *MatchingWorkerPool) Submit(ctx context.Context, transaction *models.Transaction) {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		logging.FromContext(ctx).WithFields(map[string]interface{}{
			"transaction_id": transaction.ID.String(),
		}).Error("Matching worker pool not started, dropping matching task")
		return
	}
	p.mu.Unlock()

	task := MatchingTask{Transaction: transaction}

	select {
	case p.taskChan <- task:
		// Task submitted successfully
	default:
		// Channel full, log error but don't block
		logging.FromContext(ctx).WithFields(map[string]interface{}{
			"transaction_id": transaction.ID.String(),
		}).Error("Matching worker pool queue full, dropping matching task")
	}
}

// worker is the main loop for a worker goroutine.
func (p *MatchingWorkerPool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case task, ok := <-p.taskChan:
			if !ok {
				// Channel closed, worker should exit
				return
			}
			p.processTask(task)
		case <-p.ctx.Done():
			return
		}
	}
}

// processTask processes a single matching task.
// Any errors are logged but do not propagate to the caller.
func (p *MatchingWorkerPool) processTask(task MatchingTask) {
	ctx := p.ctx

	logger := logging.FromContext(ctx).WithFields(map[string]interface{}{
		"transaction_id": task.Transaction.ID.String(),
		"policy_id":      task.Transaction.PolicyID.String(),
		"side":           task.Transaction.Side,
	})

	_, err := p.orchestrator.Match(ctx, task.Transaction)
	if err != nil {
		// Log error but don't block ingestion
		logger.WithFields(map[string]interface{}{
			"error": err.Error(),
		}).Error("Matching failed for transaction")
		return
	}

	logger.Debug("Matching completed for transaction")
}

// NumWorkers returns the configured number of workers.
func (p *MatchingWorkerPool) NumWorkers() int {
	return p.numWorkers
}
