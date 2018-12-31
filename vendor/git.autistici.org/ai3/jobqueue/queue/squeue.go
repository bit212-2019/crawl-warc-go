package queue

import (
	"bytes"
	"container/heap"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	ldbopt "github.com/syndtr/goleveldb/leveldb/opt"
	ldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

// This needs to be small, there might be millions of such queues.
//
// Some of the fields are meant to be used by the priority queue
// implementation sqPriorityQueue.
type subQueue struct {
	head []byte
	rl   Ratelimiter

	qname        string
	index        int
	nextDeadline time.Time
}

func newSubqueue(db *leveldb.DB, qname []byte, rl Ratelimiter) (*subQueue, error) {
	q := &subQueue{
		rl:    rl,
		qname: string(qname),
	}

	// Find the head of the queue.
	qr := ldbutil.BytesPrefix(
		appendBytes(keyPrefixMain, keySeparator, qname, keySeparator))
	iter := db.NewIterator(qr, nil)
	defer iter.Release()

	if iter.First() {
		q.head = copyBuffer(iter.Key())
	}

	return q, iter.Error()
}

func (q *subQueue) pop(db *leveldb.DB, wb *leveldb.Batch, qname []byte) ([]byte, []byte, bool) {
	if q.head == nil {
		return nil, nil, false
	}

	key, value := q.incrementHead(db, qname)

	wb.Delete(key)

	q.rl.Inc()

	return key, value, true
}

func (q *subQueue) incrementHead(db *leveldb.DB, qname []byte) ([]byte, []byte) {
	// Use an iterator to retrieve the head row, as well as finding the
	// following one.
	iter := db.NewIterator(&ldbutil.Range{
		Start: q.head,
		Limit: appendBytes(
			keyPrefixMain, keySeparator, qname, keySeparatorPlus1),
	}, nil)

	if !iter.Next() || !bytes.Equal(iter.Key(), q.head) {
		// VERY BAD right now: the head row must exist.
		panic("can't fetch the head row")
	}

	// Pop the head value.
	key := q.head
	value := copyBuffer(iter.Value())

	// Find the next head (element after the current head).
	if iter.Next() {
		q.head = copyBuffer(iter.Key())
	} else {
		q.head = nil
	}
	iter.Release()

	return key, value
}

func (q *subQueue) getNextDeadline() (t time.Time) {
	if q.head != nil {
		t = timestampFromKey(q.head)
		if rlt := q.rl.NextDeadline(); rlt.After(t) {
			t = rlt
		}
	}
	return
}

func (q *subQueue) add(wb *leveldb.Batch, qname, data []byte, when time.Time) (key []byte, headUpdated bool) {
	key = makeElementKey(qname, when)
	if q.head == nil || bytes.Compare(key, q.head) < 0 {
		q.head = key
		headUpdated = true
	}
	wb.Put(key, data)
	return
}

// Remove an item from the subqueue, when we already know the full key.
func (q *subQueue) remove(db *leveldb.DB, wb *leveldb.Batch, qname, key []byte) bool {
	wb.Delete(key)
	if q.head != nil && bytes.Equal(q.head, key) {
		// Oh shit we are dropping the head, need to pop it instead.
		q.incrementHead(db, qname)
		return true
	}
	return false
}

// The subqueue manager keeps a list of all known isolation domains
// and the associated subQueues. It knows which queue has the 'next'
// item.
//
// Internally it is implemented as a priority queue using
// container/heap. The additional map is used to find subqueues by
// name.
//
// The type isn't goroutine-safe, you need an external lock to use it
// safely.
type sqManager struct {
	db  *leveldb.DB
	pq  sqPriorityQueue
	sub map[string]*subQueue
}

func newSqManager(db *leveldb.DB, rlFn RatelimiterFunc) (*sqManager, error) {
	p := &sqManager{
		db:  db,
		sub: make(map[string]*subQueue),
	}
	if err := p.load(rlFn); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *sqManager) load(rlFn RatelimiterFunc) error {
	iter := p.db.NewIterator(ldbutil.BytesPrefix(
		appendBytes(keyPrefixQueueList, keySeparator)),
		&ldbopt.ReadOptions{
			DontFillCache: true,
		})
	defer iter.Release()

	for iter.Next() {
		qname := iter.Value()
		sq, err := newSubqueue(p.db, qname, rlFn(qname))
		if err != nil {
			return err
		}
		p.addMem(iter.Value(), sq)
	}

	if len(p.pq) > 0 {
		log.Printf("queue manager: loaded %d domains", len(p.pq))
	}

	return iter.Error()
}

func (p *sqManager) String() string {
	var parts []string
	for _, sq := range p.pq {
		parts = append(parts, fmt.Sprintf("head=%q next_deadline=%v", sq.head, sq.nextDeadline))
	}
	return strings.Join(parts, ", ")
}

// List of subqueues that satisfies the Heap interface.
type sqPriorityQueue []*subQueue

func (l sqPriorityQueue) Len() int { return len(l) }

func (l sqPriorityQueue) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
	l[i].index = i
	l[j].index = j
}

func (l *sqPriorityQueue) Push(x interface{}) {
	n := len(*l)
	item := x.(*subQueue)
	item.index = n
	*l = append(*l, item)
}

func (l *sqPriorityQueue) Pop() interface{} {
	old := *l
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*l = old[0 : n-1]
	return item
}

func (l sqPriorityQueue) Less(i, j int) bool {
	// Elements with nil heads (empty queues) come last.
	if l[i].head == nil {
		return false
	}
	if l[j].head == nil {
		return true
	}

	// Order by next execution deadline.
	di := l[i].nextDeadline
	dj := l[j].nextDeadline
	return di.Before(dj)
}

// Add a subQueue to the in-memory registry.
func (p *sqManager) addMem(qname []byte, sq *subQueue) {
	sq.nextDeadline = sq.getNextDeadline()
	heap.Push(&p.pq, sq)
	p.sub[string(qname)] = sq
}

// Add a new subQueue to the in-memory registry and to the on-disk
// queue list.
func (p *sqManager) add(qname []byte, sq *subQueue) error {
	p.addMem(qname, sq)
	return p.db.Put(makeQueueListKey(qname), qname, nil)
}

// Get the subQueue given its name.
func (p *sqManager) get(qname []byte) (*subQueue, bool) {
	sq, ok := p.sub[string(qname)]
	if !ok {
		return nil, false
	}
	return sq, true
}

// Returns information on the next available queue (the head of the priority
// queue): name, subQueue, and next deadline.
func (p *sqManager) head() ([]byte, *subQueue, time.Time) {
	sq := p.pq[0]
	return []byte(sq.qname), sq, sq.nextDeadline
}

// Update a subQueue, reshuffling the priority queue given its next deadline.
// Returns a boolean indicating whether the head element nextDeadline has
// changed or not.
func (p *sqManager) update(qname []byte, sq *subQueue) bool {
	oldHeadDeadline := p.pq[0].nextDeadline
	sq.nextDeadline = sq.getNextDeadline()
	heap.Fix(&p.pq, sq.index)
	return p.pq[0].nextDeadline != oldHeadDeadline
}

func (p *sqManager) empty() bool { return len(p.pq) == 0 }
