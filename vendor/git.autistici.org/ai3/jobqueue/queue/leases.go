package queue

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
)

var keyPrefixLeases = []byte("l")

// Associate a lease with the key of a future event that will cause
// its expiration. The lease mechanism is very simple and works like
// this:
//
// - when returning an entry from the queue, we create a new lease
//   ID and return it to the caller along with the entry
// - we write the same entry back in the queue with a timestamp
//   in the future
// - we also write a row in the db with the lease ID
// - the caller can call ping(leaseID), in which case we
//   remove the previous entry from the queue, and we add it
//   back with a timestamp a bit more in the future
// - if we don't hear back, the entry will be automatically
//   reassigned
// - periodically, we must get rid of old leaseIDs...

func newLeaseID() uint64 {
	return rand.Uint64()
}

func makeLeaseKey(id uint64) []byte {
	return appendBytes(keyPrefixLeases, keySeparator, encodeUint64(id))
}

func encodeUint64(i uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], i)
	return b[:]
}

// LeaseQueue is a Queue extended with the concept of 'leases', meant
// to support remote (or otherwise unreliable) subscribers, and to
// provide retry semantics that work reliably in face of varying task
// complexities.
//
// Whenever a subscriber acknowledges receipt of an item by calling
// Ack(), a new 'retry' item is added to the queue scheduled at some
// point in the future. If the subscriber fails to call Done() within
// that time, the task is automatically retried. The lease duration
// can be periodically extended by calling Keepalive().
//
// The same mechanism used to handle dead subscribers can be used to
// deal with temporary failures: just abort the task without calling
// Done() and it will be retried in the future.
//
// The lease expiration time is set globally for the entire LeaseQueue
// (default 5 minutes).
type LeaseQueue struct {
	*Queue
}

// NewLeaseQueue creates a new LeaseQueue.
func NewLeaseQueue(db *leveldb.DB, opts ...Option) (*LeaseQueue, error) {
	q, err := NewQueue(db, opts...)
	if err != nil {
		return nil, err
	}
	return &LeaseQueue{Queue: q}, nil
}

// Ack is called when the consumer has received an item, creating a
// new lease for it.
func (q *LeaseQueue) Ack(item Item) (uint64, error) {
	qitem := item.(*queueItem)
	retryKey, err := q.Queue.Add(qitem.qname(), qitem.data, time.Now().Add(q.retryInterval))
	if err != nil {
		return 0, err
	}
	leaseID := newLeaseID()
	if err := q.db.Put(makeLeaseKey(leaseID), retryKey, nil); err != nil {
		return 0, err
	}
	return leaseID, q.Queue.Ack(item)
}

// Keepalive extends a lease when the consumer is still working on an item.
func (q *LeaseQueue) Keepalive(leaseID uint64) error {
	oldRetryKey, err := q.db.Get(makeLeaseKey(leaseID), nil)
	if err != nil {
		return fmt.Errorf("lease %x does not exist: %v", leaseID, err)
	}
	data, err := q.db.Get(oldRetryKey, nil)
	if err != nil {
		return fmt.Errorf("row pointed by lease (%q) does not exist: %v", oldRetryKey, err)
	}

	qname := qnameFromKey(oldRetryKey)
	retryKey, err := q.Queue.Add(qname, data, time.Now().Add(q.retryInterval))
	if err != nil {
		return err
	}
	if err := q.db.Put(makeLeaseKey(leaseID), retryKey, nil); err != nil {
		return err
	}
	return q.Queue.remove(oldRetryKey)
}

// Done is called when a consumer is done working on the item.
func (q *LeaseQueue) Done(leaseID uint64) error {
	oldRetryKey, err := q.db.Get(makeLeaseKey(leaseID), nil)
	if err != nil {
		return fmt.Errorf("lease %x does not exist: %v", leaseID, err)
	}

	if err := q.db.Delete(makeLeaseKey(leaseID), nil); err != nil {
		return err
	}

	return q.Queue.remove(oldRetryKey)
}
