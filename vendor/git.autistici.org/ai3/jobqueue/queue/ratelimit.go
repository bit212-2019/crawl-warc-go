package queue

import "time"

// Ratelimiter interface. Inc() increments the internal counter,
// NextDeadline() returns the next possible time for scheduling an
// event. NextDeadline() can be called multiple times, so it should
// probably just return an internal value that is modified by Inc().
type Ratelimiter interface {
	Inc()
	NextDeadline() time.Time
}

// RatelimiterFunc is a function that returns a new Ratelimiter given
// a specific domain. There is no need to cache results, the function
// will only be called once for each domain.
type RatelimiterFunc func([]byte) Ratelimiter

// The nullRatelimiter does not do anything.
type nullRatelimiter struct{}

func (l nullRatelimiter) Inc()                    {}
func (l nullRatelimiter) NextDeadline() time.Time { return time.Time{} }

// Keep a shared nullRatelimiter object around to be reused.
var defaultRateLimiter = new(nullRatelimiter)

// Simple rate limiter, waits 1/qps seconds.
type simpleRatelimiter struct {
	period time.Duration
	next   time.Time
}

// NewSimpleRatelimiter returns a Ratelimiter that emits a continuous
// stream of entries at the desired qps. Probably loses accuracy at
// very high qps.
func NewSimpleRatelimiter(qps float64) Ratelimiter {
	return &simpleRatelimiter{
		period: time.Duration(1000000000/qps) * time.Nanosecond,
	}
}

func (r *simpleRatelimiter) Inc() {
	now := time.Now()
	next := r.next.Add(r.period)
	if next.Before(now) {
		next = now
	}
	r.next = next
}

func (r *simpleRatelimiter) NextDeadline() time.Time { return r.next }
