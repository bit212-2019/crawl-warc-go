// Package queue offers a simple but performant queue with rate-limiting
// capability. The queue is persisted to disk as a LevelDB database.
//
// The package defines two queue types: Queue and LeasedQueue. The former only
// supports simple delivery acknowledgement, while the latter adds support for
// leases - which can detect dead or temporarily failed subscribers - and
// provides a good foundation for a distributed task processing system.
//
// Rate limiting operates on separate user-defined "domains": items added to
// the queue will be tagged with a domain tag, and the queue will not output
// items for a domain at a rate higher than requested, while at the same time
// it will try to maximize subscriber saturation. The cost of adding domains
// is meant to be extremely low.
//
// Tagged rate limiting is normally used when tasks involve access to limited
// shared (or third-party) resources. Considering the common case of
// distributed workers, implementing rate limiting in the queue removes the
// need for a distributed rate limiting solution, and is particularly suited
// to handle large numbers (unknown / unbounded) of low-qps rate limits.
//
package queue

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	ldbopt "github.com/syndtr/goleveldb/leveldb/opt"
	ldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	keyPrefixMain      = []byte("q")
	keyPrefixActive    = []byte("qa")
	keyPrefixQueueList = []byte("ql")
	keySeparator       = []byte("/")
	keySeparatorPlus1  = []byte("0") // keySeparator[0] + 1

	errInterrupted = errors.New("interrupted")
	errStopped     = errors.New("stopped")
	errDone        = errors.New("done")

	zeroSlice = []byte{}
)

// Item returned from a Queue.
type Item interface {
	Data() []byte
}

// Items returned from the queue. The sync.Mutex is used to implement safe
// acknowledgement on part of the (local) consumer.
type queueItem struct {
	mx        sync.Mutex
	activeKey []byte
	data      []byte
}

// Create a new, locked, queueItem.
func newQueueItem(key, value []byte) *queueItem {
	item := &queueItem{activeKey: key, data: value}
	item.mx.Lock()
	return item
}

// Data returns the item data.
func (i *queueItem) Data() []byte { return i.data }

func (i *queueItem) qname() []byte {
	return qnameFromKey(keyFromActiveKey(i.activeKey))
}

// Option represents an option for NewQueue.
type Option func(*Queue)

// OneShot makes the Queue exit when done (no pending tasks).
func OneShot(oneshot bool) Option {
	return func(q *Queue) {
		q.oneShot = oneshot
	}
}

// WithRatelimiter sets a custom Ratelimiter for a queue.
func WithRatelimiter(rlFn RatelimiterFunc) Option {
	return func(q *Queue) {
		q.rlFn = rlFn
	}
}

// WithRetryInterval sets the task retry interval on temporary failures.
func WithRetryInterval(d time.Duration) Option {
	return func(q *Queue) {
		q.retryInterval = d
	}
}

// Queue with multiple ratelimited domains, backed by persistent storage.
type Queue struct {
	db *leveldb.DB

	mx   sync.Mutex
	subs *sqManager

	numActive  uint64
	sqNotifyCh chan struct{}
	stopCh     chan struct{}
	doneCh     chan struct{}
	dispatchCh chan Item
	wakeupCh   chan struct{}
	err        error

	rlFn          RatelimiterFunc
	retryInterval time.Duration
	oneShot       bool
}

func defaultRlFn(_ []byte) Ratelimiter {
	return defaultRateLimiter
}

// NewQueue creates a new Queue. Use options to specify parameters
// that differ from the defaults.
func NewQueue(db *leveldb.DB, opts ...Option) (*Queue, error) {
	q := &Queue{
		db:            db,
		sqNotifyCh:    make(chan struct{}, 1),
		dispatchCh:    make(chan Item, 100),
		doneCh:        make(chan struct{}),
		wakeupCh:      make(chan struct{}, 1),
		stopCh:        make(chan struct{}),
		rlFn:          defaultRlFn,
		oneShot:       true,
		retryInterval: 5 * time.Minute,
	}

	for _, opt := range opts {
		opt(q)
	}

	var err error
	if err = recoverActiveEntries(db); err != nil {
		return nil, err
	}

	q.subs, err = newSqManager(db, q.rlFn)
	if err != nil {
		return nil, err
	}

	return q, nil
}

// Subscribe returns a channel that returns Items when they are available.
func (q *Queue) Subscribe() <-chan Item {
	return q.dispatchCh
}

func recoverActiveEntries(db *leveldb.DB) error {
	// Move everything from the 'pending delivery' queue back to the
	// respective queues.  Do this before initializing the in-memory
	// subqueues, so we don't have to update them.
	wb := new(leveldb.Batch)
	iter := db.NewIterator(
		ldbutil.BytesPrefix(appendBytes(keyPrefixActive, keySeparator)),
		&ldbopt.ReadOptions{
			DontFillCache: true,
		})
	defer iter.Release()
	var count int
	for iter.Next() {
		origKey := keyFromActiveKey(iter.Key())
		wb.Delete(iter.Key())
		wb.Put(origKey, iter.Value())
		count++
	}
	if count > 0 {
		log.Printf("recovering %d active entries", count)
	}
	return db.Write(wb, nil)
}

// Start the queue.
func (q *Queue) Start() {
	go q.loop()
}

// Stop the queue, wait for safe termination.
func (q *Queue) Stop() error {
	close(q.stopCh)
	<-q.doneCh
	return q.err
}

// Loop forever, or until stopped.
func (q *Queue) loop() {
	defer close(q.dispatchCh)
	defer close(q.doneCh)
	for {
		if err := q.tick(); err != nil {
			if err != errStopped && err != errDone {
				q.err = err
				log.Printf("tick() error: %v", err)
			}
			return
		}
	}
}

// Ack acknowledges that the consumer has received an item. Removes
// the entry from the list of entries pending delivery.
func (q *Queue) Ack(item Item) error {
	atomic.AddUint64(&q.numActive, ^uint64(0))
	qitem := item.(*queueItem)
	qitem.mx.Lock()
	defer qitem.mx.Unlock() // not really necessary though
	return q.db.Delete(qitem.activeKey, nil)
}

// Nack reports an error processing the item and requests for it to be
// rescheduled some time in the future.
func (q *Queue) Nack(item Item) error {
	atomic.AddUint64(&q.numActive, ^uint64(0))
	qitem := item.(*queueItem)
	qitem.mx.Lock()
	defer qitem.mx.Unlock()

	_, err := q.Add(qitem.qname(), qitem.data, time.Now().Add(q.retryInterval))
	if err != nil {
		return err
	}
	return q.db.Delete(qitem.activeKey, nil)
}

// Add an entry to the queue, to be scheduled after the specified time. The
// domain tag can be nil.
//
// nolint: gocyclo
func (q *Queue) Add(qname, data []byte, when time.Time) ([]byte, error) {
	if qname == nil {
		qname = zeroSlice
	}

	q.mx.Lock()
	defer q.mx.Unlock()

	sq, ok := q.subs.get(qname)
	if !ok {
		// Create the in-memory subQueue object.
		var err error
		sq, err = newSubqueue(q.db, qname, q.rlFn(qname))
		if err != nil {
			return nil, err
		}
	}

	// subQueue.add() takes a Batch, so create one just for it.
	wb := new(leveldb.Batch)
	key, wakeup := sq.add(wb, qname, data, when)
	if err := q.db.Write(wb, nil); err != nil {
		return nil, err
	}

	// Now that we've added the element to the database, update
	// our subqueue tracker if necessary.
	if !ok {
		if err := q.subs.add(qname, sq); err != nil {
			return nil, err
		}

		// Wakeup nextSubqueue(), we have a new domain.
		select {
		case q.sqNotifyCh <- struct{}{}:
		default:
		}
	}

	// If the subqueue head was updated, we need to reprioritize the
	// subqueue. When the global head changes, wake up
	// interruptibleSleep() which may be waiting on the old head.
	if wakeup && q.subs.update(qname, sq) {
		q.wakeup()
	}

	return key, nil
}

// Remove an entry from the queue given its full database key.
func (q *Queue) remove(key []byte) error {
	q.mx.Lock()
	defer q.mx.Unlock()

	qname := qnameFromKey(key)
	sq, ok := q.subs.get(qname)
	if !ok {
		return errors.New("no such queue")
	}
	wb := new(leveldb.Batch)
	wakeup := sq.remove(q.db, wb, qname, key)
	if err := q.db.Write(wb, nil); err != nil {
		return err
	}
	if wakeup && q.subs.update(qname, sq) {
		q.wakeup()
	}
	return nil
}

// Wake up tick() if it's sleeping.
func (q *Queue) wakeup() {
	select {
	case q.wakeupCh <- struct{}{}:
	default:
	}
}

// Sleep until deadline, unless we are woken up or stopped.
// Returns true if we were interrupted, errStopped on stop.
func (q *Queue) interruptibleSleep(deadline time.Time) error {
	t := time.NewTimer(time.Until(deadline))
	defer t.Stop()
	select {
	case <-q.stopCh:
		return errStopped
	case <-q.wakeupCh:
		return errInterrupted
	case <-t.C:
		return nil
	}
}

// Intervals smaller than this are short-circuited into a busy loop.
var dontEvenBother = 1 * time.Millisecond

// Return the next subQueue that has available data and the associated deadline.
func (q *Queue) nextAvailableSubqueue() ([]byte, *subQueue, time.Time, error) {
	// No subqueues available? Then wait. Only triggered in the
	// initial stage of the service.
	q.mx.Lock()
	if q.subs.empty() {
		q.mx.Unlock()
		select {
		case <-q.stopCh:
			return nil, nil, time.Time{}, errStopped
		case <-q.sqNotifyCh:
		}
		q.mx.Lock()
	}
	defer q.mx.Unlock()

	// Get the head of the priority queue.
	qname, sq, timestamp := q.subs.head()
	return qname, sq, timestamp, nil
}

func (q *Queue) waitUntilNextAvailableSubqueue() ([]byte, *subQueue, error) {
	// Find the next subqueue, or wait indefinitely until there is one.
	qname, sq, deadline, err := q.nextAvailableSubqueue()
	if err != nil {
		return nil, nil, err
	}

	// Wait until there is an element ready on the subqueue.
	now := time.Now()
	if deadline.After(now) {
		diff := deadline.Sub(now)
		if diff > dontEvenBother {
			if err := q.interruptibleSleep(deadline); err != nil {
				return nil, nil, err
			}
		}
	}

	return qname, sq, nil
}

func (q *Queue) tick() error {
	qname, sq, err := q.waitUntilNextAvailableSubqueue()
	if err == errInterrupted {
		return nil
	} else if err != nil {
		return err
	}

	// Process the next element, send it to our dispatch queue. It's ok to
	// be limited by the retrieval rate at this point.
	//
	// Note that since we have released the lock in the meantime, there is
	// no connection between the *value* that was retrieved by peek()
	// above and the one we're going to get with pop() below. But it
	// doesn't matter, because all the logic up to now is only used to
	// figure out which queue to wait on, and by now the current queue can
	// return a value even if another queue has pre-empted it in the
	// priority queue.
	//
	// We only write changes to the database *after* the item has been
	// delivered (which might take an arbitrarily long time, or fail).
	// The Batch is used to defer the write operation. At the same time,
	// we write the data in the queue of items pending delivery, to
	// implement our side of the safe acknowledgement protocol.
	q.mx.Lock()
	wb := new(leveldb.Batch)
	key, value, ok := sq.pop(q.db, wb, qname)
	activeKey := makeActiveElementKey(key)
	wb.Put(activeKey, value)
	q.mx.Unlock()
	if !ok {
		// All queues are empty. If this isn't a one-shot run,
		// we can simply wait a bit for something to happen.
		numActive := atomic.LoadUint64(&q.numActive)
		if q.oneShot && numActive == 0 {
			return errDone
		}
		// Wait a little bit (or until we are stopped).
		c := time.After(100 * time.Millisecond)
		select {
		case <-c:
			return nil
		case <-q.stopCh:
			return errStopped
		}
	}

	// Create a new item, synchronized (mutex starts locked) and deliver
	// it to consumers. This might block indefinitely.
	item := newQueueItem(activeKey, value)
	select {
	case q.dispatchCh <- item:
	case <-q.stopCh:
		return errStopped
	}

	// Now that we have delivered the item, update the associated queue
	// and write changes to the database.
	q.mx.Lock()
	q.subs.update(qname, sq)
	if err := q.db.Write(wb, nil); err != nil {
		return err
	}
	q.mx.Unlock()
	atomic.AddUint64(&q.numActive, 1)

	// Ensure that the consumer can't call ack() before this point.
	item.mx.Unlock()

	return nil
}

func makeElementKey(qname []byte, when time.Time) []byte {
	return appendBytes(keyPrefixMain, keySeparator, qname, keySeparator, monotonicID(when))
}

func makeActiveElementKey(key []byte) []byte {
	return appendBytes(keyPrefixActive, keySeparator, key)
}

func makeQueueListKey(qname []byte) []byte {
	return appendBytes(keyPrefixQueueList, keySeparator, qname)
}

var uniqueCounter uint32

// Return a monotonic, unique id, based on timestamp and a local
// in-process counter. The counter is reset whenever the process
// restarts. We could have used the nanoseconds part of the timestamp,
// but we prefer deterministic uniqueness.
func monotonicID(when time.Time) []byte {
	var buf [12]byte
	if !when.IsZero() {
		binary.BigEndian.PutUint64(buf[:8], uint64(when.UnixNano()))
	}
	cntr := atomic.AddUint32(&uniqueCounter, 1)
	binary.BigEndian.PutUint32(buf[8:], cntr)
	return buf[:]
}

func timestampFromKey(k []byte) time.Time {
	parts := bytes.SplitN(k, keySeparator, 3)
	if len(parts) < 3 {
		panic(fmt.Sprintf("malformed key %q", k))
	}
	benc := parts[2]
	if len(benc) < 8 {
		panic(fmt.Sprintf("bad timestamp part of key %q, key %q", benc, k))
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(benc[:8])))
}

func keyFromActiveKey(k []byte) []byte {
	return k[len(keyPrefixActive)+len(keySeparator):]
}

func qnameFromKey(k []byte) []byte {
	return bytes.Split(k, keySeparator)[1]
}

func copyBuffer(b []byte) []byte {
	// out := make([]byte, len(b))
	// copy(out, b)
	// return out
	return append([]byte(nil), b...)
}

func appendBytes(b ...[]byte) []byte {
	return bytes.Join(b, nil)
}
