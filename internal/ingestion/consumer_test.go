package ingestion

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrRetryable(t *testing.T) {
	t.Run("NewRetryableError wraps error correctly", func(t *testing.T) {
		originalErr := errors.New("connection timeout")
		err := NewRetryableError(originalErr)

		assert.True(t, errors.Is(err, ErrRetryable), "error should be identified as ErrRetryable")
		assert.False(t, errors.Is(err, ErrFatal), "error should not be identified as ErrFatal")
		assert.Equal(t, "connection timeout", err.Error())
	})

	t.Run("NewRetryableError with nil returns nil", func(t *testing.T) {
		err := NewRetryableError(nil)
		assert.Nil(t, err)
	})

	t.Run("ErrRetryable can be unwrapped", func(t *testing.T) {
		originalErr := errors.New("original error")
		err := NewRetryableError(originalErr)

		unwrapped := errors.Unwrap(err)
		assert.Equal(t, originalErr, unwrapped)
	})
}

func TestErrFatal(t *testing.T) {
	t.Run("NewFatalError wraps error correctly", func(t *testing.T) {
		originalErr := errors.New("invalid message format")
		err := NewFatalError(originalErr)

		assert.True(t, errors.Is(err, ErrFatal), "error should be identified as ErrFatal")
		assert.False(t, errors.Is(err, ErrRetryable), "error should not be identified as ErrRetryable")
		assert.Equal(t, "invalid message format", err.Error())
	})

	t.Run("NewFatalError with nil returns nil", func(t *testing.T) {
		err := NewFatalError(nil)
		assert.Nil(t, err)
	})

	t.Run("ErrFatal can be unwrapped", func(t *testing.T) {
		originalErr := errors.New("original error")
		err := NewFatalError(originalErr)

		unwrapped := errors.Unwrap(err)
		assert.Equal(t, originalErr, unwrapped)
	})
}

func TestErrorsAreDistinguishable(t *testing.T) {
	t.Run("ErrRetryable and ErrFatal are distinguishable via errors.Is", func(t *testing.T) {
		retryableErr := NewRetryableError(errors.New("retry me"))
		fatalErr := NewFatalError(errors.New("fatal"))

		// Verify they are distinguishable
		assert.True(t, errors.Is(retryableErr, ErrRetryable))
		assert.False(t, errors.Is(retryableErr, ErrFatal))

		assert.True(t, errors.Is(fatalErr, ErrFatal))
		assert.False(t, errors.Is(fatalErr, ErrRetryable))
	})

	t.Run("sentinel errors are not equal to each other", func(t *testing.T) {
		assert.False(t, errors.Is(ErrRetryable, ErrFatal))
		assert.False(t, errors.Is(ErrFatal, ErrRetryable))
	})
}
