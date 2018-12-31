package queue

import (
	"context"
	"io"
	"log"
	"time"

	jq "git.autistici.org/ai3/jobqueue"
)

// Local queue client.
type localClient struct {
	q *Queue
}

func (c *localClient) Add(_ context.Context, qname, data []byte) error {
	_, err := c.q.Add(qname, data, time.Time{})
	return err
}

func (c *localClient) Next(ctx context.Context) (jq.Job, error) {
	var item Item
	select {
	case item = <-c.q.Subscribe():
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if item == nil {
		return nil, io.EOF
	}
	return &localClientJob{
		Item: item,
		q:    c.q,
	}, nil
}

func (c *localClient) Close() {}

type localClientJob struct {
	Item
	q *Queue
}

func (j *localClientJob) Done(_ context.Context, err error) error {
	if err != nil {
		return j.q.Nack(j.Item)
	}
	return j.q.Ack(j.Item)
}

// Client returns a local, in-process client for this queue.
func (q *Queue) Client() jq.Queue {
	return &localClient{q: q}
}

// Client for local LeaseQueue.
type localLeaseClient struct {
	q *LeaseQueue

	keepaliveInterval time.Duration
}

func (c *localLeaseClient) Add(_ context.Context, qname, data []byte) error {
	_, err := c.q.Add(qname, data, time.Time{})
	return err
}

func (c *localLeaseClient) Next(ctx context.Context) (jq.Job, error) {
	var item Item
	select {
	case item = <-c.q.Subscribe():
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if item == nil {
		return nil, io.EOF
	}

	leaseID, err := c.q.Ack(item)
	if err != nil {
		return nil, err
	}

	j := &localLeaseClientJob{
		Item:          item,
		leaseID:       leaseID,
		q:             c.q,
		stopCh:        make(chan struct{}),
		keepaliveDone: make(chan struct{}),
	}
	go j.keepalive(c.keepaliveInterval)
	return j, nil
}

func (c *localLeaseClient) Close() {}

type localLeaseClientJob struct {
	Item

	leaseID       uint64
	q             *LeaseQueue
	stopCh        chan struct{}
	keepaliveDone chan struct{}
}

func (j *localLeaseClientJob) keepalive(keepaliveInterval time.Duration) {
	defer close(j.keepaliveDone)
	ticker := time.NewTicker(keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := j.q.Keepalive(j.leaseID); err != nil {
				log.Printf("lease %016x: keepalive error: %v", j.leaseID, err)
			}
		case <-j.stopCh:
			return
		}
	}
}

func (j *localLeaseClientJob) Done(_ context.Context, err error) error {
	close(j.stopCh)
	<-j.keepaliveDone

	if err == nil {
		return j.q.Done(j.leaseID)
	}
	return nil
}

// Client returns a local, in-process client for this queue.
func (q *LeaseQueue) Client() jq.Queue {
	return &localLeaseClient{
		q:                 q,
		keepaliveInterval: q.Queue.retryInterval / 3,
	}
}
