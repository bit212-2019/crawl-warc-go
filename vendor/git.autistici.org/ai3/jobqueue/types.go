// Package jobqueue offers a simple interface to a (persistent) queue
// that can transparently scale, in API terms, from the embedded case
// to the remote distributed service scenario.
//
package jobqueue

import "context"

// Queue is a client interface to a (local or remote) queue. The
// interface is targeted towards job-based workflows
type Queue interface {
	// Add a new tagged element to the queue.
	Add(context.Context, []byte, []byte) error

	// Retrieve an element from the queue. This call will block.
	Next(context.Context) (Job, error)

	// Close this client and all associated resources.
	Close()
}

type Job interface {
	// Job data.
	Data() []byte

	// Call when processing is done. All errors are temporary.
	Done(context.Context, error) error
}
