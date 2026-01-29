package ingestion

import (
	"context"
	"errors"
)

// ErrRetryable indicates an error that can be retried.
// Use errors.Is(err, ErrRetryable) to check if an error is retryable.
var ErrRetryable = errors.New("retryable error")

// ErrFatal indicates a fatal error that should not be retried.
// Use errors.Is(err, ErrFatal) to check if an error is fatal.
var ErrFatal = errors.New("fatal error")

// ErrWrongTopic indicates that a message was delivered to the wrong topic handler.
// This happens due to NATS queue group message distribution when multiple topics
// share the same consumer. The message should be Nacked for redelivery.
// Use errors.Is(err, ErrWrongTopic) to check if an error is a wrong topic error.
var ErrWrongTopic = errors.New("wrong topic error")

// NewRetryableError wraps an error as retryable.
func NewRetryableError(err error) error {
	if err == nil {
		return nil
	}
	return &retryableError{err: err}
}

// NewFatalError wraps an error as fatal.
func NewFatalError(err error) error {
	if err == nil {
		return nil
	}
	return &fatalError{err: err}
}

// NewWrongTopicError wraps an error as a wrong topic error.
// Use this when a message was delivered to the wrong handler due to
// NATS queue group message distribution.
func NewWrongTopicError(err error) error {
	if err == nil {
		return nil
	}
	return &wrongTopicError{err: err}
}

type retryableError struct {
	err error
}

func (e *retryableError) Error() string {
	return e.err.Error()
}

func (e *retryableError) Is(target error) bool {
	return target == ErrRetryable
}

func (e *retryableError) Unwrap() error {
	return e.err
}

type fatalError struct {
	err error
}

func (e *fatalError) Error() string {
	return e.err.Error()
}

func (e *fatalError) Is(target error) bool {
	return target == ErrFatal
}

func (e *fatalError) Unwrap() error {
	return e.err
}

type wrongTopicError struct {
	err error
}

func (e *wrongTopicError) Error() string {
	return e.err.Error()
}

func (e *wrongTopicError) Is(target error) bool {
	return target == ErrWrongTopic
}

func (e *wrongTopicError) Unwrap() error {
	return e.err
}

// Event represents an event to be processed by the consumer.
type Event interface{}

// Consumer defines the interface for event consumers.
type Consumer interface {
	// Start begins consuming events. It blocks until the context is cancelled
	// or an error occurs.
	Start(ctx context.Context) error
	// Stop gracefully stops the consumer.
	Stop()
	// Health returns nil if the consumer is healthy, or an error describing
	// the health issue.
	Health() error
}

// EventHandler defines the interface for handling events.
type EventHandler interface {
	// Handle processes an event. Returns ErrRetryable if the event should be
	// retried, ErrFatal if processing should stop, or nil on success.
	Handle(ctx context.Context, event Event) error
}
