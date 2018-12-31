Job queue
===

An efficient and relatively fast and robust queue with the following
characteristics:

* persistent, must survive restarts and recover;

* keyed by time, elements added to the queue can specify a minimum
  timestamp for scheduling - this allows us to retry jobs on error
  after a delay;

* rate-limiting by domain, every element can have an associated
  rate-limiting domain, and the queue will pop elements according to
  the desired rate limit. The list of possible domains is not known in
  advance;

* detect dead jobs using *leases* with keepalives, which works both
  for short-running and long-running jobs without making assumptions
  on their expected duration.

This package offers a Go API to embed queues inside your application.

## Usage

The API attempts to *scale transparently from in-process queues to
remote, distributed ones*. By this we mean using interfaces to
decouple the environment setup from the application logic.

For instance, this would be a typical "worker" flow:

```go
func worker(ctx context.Context, queue jobqueue.Queue) error {
        job, err := queue.Next(ctx)
        if err != nil {
                return err
        }
        // Do work with 'job'...
        return job.Done(ctx, nil)
}
```

The setup is then handled when creating the *Queue* client object, for
which we have various alternatives.

### In-process queue

An in-process queue has minimal overhead: every client interacts
directly with the database, the queues are implemented with Go
channels.

Clearly in this case you will also need to create the actual queue
implementation *queue.Queue* object within the application.

Example setup:

```go
// Create the in-process Queue implementation.
q, _ := queue.NewQueue(db)
// Use its Client() method to get a client that we can pass to worker().
client := q.Client()
```

### Remote queue

For distributed setups (meaning: distributed workers, but still a
centralized queue scheduler), we're using GRPC for inter-process
communication.

Example setup:

```go
// This returns a Queue client object that we can pass to worker().
client, err := net.NewClient("myserver:1234")
```

While on the server:

```go
q, _ := queue.NewLeaseQueue()
grpcServer := grpc.NewServer()
pb.RegisterQueueServiceServer(grpcServer, net.NewServer(q))
grpcServer.Serve(listener)
```

Note that the server must use the *queue.LeaseQueue* implementation in
order to properly support dead worker detection etc.


## Design

The queue is built on top of a key-value store with fast range
iteration (LevelDB is a reasonable starting point).

Domain ratelimiting can be implemented with separate per-domain
queues, and discovering new domains at element insertion time. Then
we can maintain a domain map at runtime (conveniently overlaps with
the rate-limiter buckets storage):

> *domain* -> { *ratelimiter*, *queue\_head* }

> *ratelimiter* -> { *next\_available\_timestamp* }

The domain map can then be kept ordered by *next_available_timestamp*,
so that the queue *next()* method can quickly find a non-ratelimited,
non-empty queue. In pseudo-code:

```
for domain in ordered_domains:
    if domain.next_available_timestamp() < now() and not domain.empty():
        domain.ratelimit_incr()
        return domain.next_nonblock()
```

The queue key structure must then be the following:

> *domain* / *timestamp* / *unique\_id*

