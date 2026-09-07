package pgserver

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCopyToStdoutCancellationPreservesProducerError guards the error path
// used when the COPY reader cancels the context after recording its failure.
// The setup error must not mask the concrete producer/transport error.
func TestCopyToStdoutCancellationPreservesProducerError(t *testing.T) {
	ctxErr := context.Canceled
	setupErr := errors.New("writer setup failed")
	producerErr := errors.New("copy transport failed")

	var globalErr atomic.Pointer[error]
	globalErr.Store(&producerErr)

	// Keep the setup error in scope to make sure the stored error, not the
	// stale local, is returned by the production cancellation helper.
	got := copyToStdoutCancellationError(ctxErr, &globalErr)

	require.ErrorIs(t, got, ctxErr)
	require.ErrorIs(t, got, producerErr)
	require.NotErrorIs(t, got, setupErr)
}
