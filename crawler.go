package crawl

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	jq "git.autistici.org/ai3/jobqueue"
	"git.autistici.org/ai3/jobqueue/queue"
	"github.com/PuerkitoBio/purell"
	"github.com/syndtr/goleveldb/leveldb"
	lerr "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	// ErrorRetryDelay is how long we're going to wait before retrying a
	// task that had a temporary error.
	ErrorRetryDelay = 180 * time.Second

	// LevelDBWriteBufferSize is the size of the levelDB write buffer
	// (higher than the default as the workload is write-intensive).
	LevelDBWriteBufferSize = 32 * 1024 * 1024
)

// gobDB is a very thin layer on top of LevelDB that serializes
// objects using encoding/gob.
type gobDB struct {
	*leveldb.DB
}

func newGobDB(path string) (*gobDB, error) {
	db, err := leveldb.OpenFile(path, &opt.Options{
		OpenFilesCacheCapacity: 1000,
		WriteBuffer:            LevelDBWriteBufferSize,
	})
	if lerr.IsCorrupted(err) {
		log.Printf("corrupted database, recovering...")
		db, err = leveldb.RecoverFile(path, nil)
	}
	if err != nil {
		return nil, err
	}
	return &gobDB{db}, nil
}

func (db *gobDB) PutObj(key []byte, obj interface{}) error {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(obj); err != nil {
		return err
	}
	return db.DB.Put(key, b.Bytes(), nil)
}

func (db *gobDB) GetObj(key []byte, obj interface{}) error {
	data, err := db.Get(key, nil)
	if err != nil {
		return err
	}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(obj); err != nil {
		return err
	}
	return nil
}

// Outlink is a tagged outbound link.
type Outlink struct {
	URL *url.URL
	Tag int
}

const (
	// TagPrimary is a primary reference (another web page).
	TagPrimary = iota

	// TagRelated is a secondary resource, related to a page.
	TagRelated
)

// URLInfo stores information about a crawled URL.
type URLInfo struct {
	URL        string
	StatusCode int
	CrawledAt  time.Time
	Error      string
}

// A Fetcher retrieves contents from remote URLs.
type Fetcher interface {
	// Fetch retrieves a URL and returns the response.
	Fetch(string) (*http.Response, error)
}

// FetcherFunc wraps a simple function into the Fetcher interface.
type FetcherFunc func(string) (*http.Response, error)

// Fetch retrieves a URL and returns the response.
func (f FetcherFunc) Fetch(u string) (*http.Response, error) {
	return f(u)
}

// A Handler processes crawled contents. Any errors returned by public
// implementations of this interface are considered fatal and will
// cause the crawl to abort. The URL will be removed from the queue
// unless the handler returns the special error ErrRetryRequest.
type Handler interface {
	// Handle the response from a URL.
	Handle(*Crawler, string, int, *http.Response, error) error
}

// HandlerFunc wraps a function into the Handler interface.
type HandlerFunc func(*Crawler, string, int, *http.Response, error) error

// Handle the response from a URL.
func (f HandlerFunc) Handle(db *Crawler, u string, depth int, resp *http.Response, err error) error {
	return f(db, u, depth, resp, err)
}

// ErrRetryRequest is returned by a Handler when the request should be
// retried after some time.
var ErrRetryRequest = errors.New("retry_request")

// The Crawler object contains the crawler state.
type Crawler struct {
	db      *gobDB
	queue   jq.Queue
	seeds   []*url.URL
	scope   Scope
	fetcher Fetcher
	handler Handler

	workerCtx   context.Context
	stopWorkers context.CancelFunc

	enqueueMx sync.Mutex
}

func normalizeURL(u *url.URL) *url.URL {
	urlStr := purell.NormalizeURL(u,
		purell.FlagsSafe|purell.FlagRemoveDotSegments|purell.FlagRemoveDuplicateSlashes|
			purell.FlagRemoveFragment|purell.FlagSortQuery)
	u2, err := url.Parse(urlStr)
	if err != nil {
		// We *really* do not expect an error here.
		panic(err)
	}
	return u2
}

var defaultRPCTimeout = 30 * time.Second

type queueItem struct {
	URL   string
	Depth int
}

// Enqueue a (possibly new) URL for processing.
func (c *Crawler) Enqueue(link Outlink, depth int) error {
	// Normalize the URL. We are going to replace link.URL in-place, to
	// ensure that scope checks are applied to the normalized URL.
	link.URL = normalizeURL(link.URL)

	// See if it's in scope.
	if !c.scope.Check(link, depth) {
		return nil
	}

	// Protect the read-modify-update below with a mutex.
	c.enqueueMx.Lock()
	defer c.enqueueMx.Unlock()

	// Check if we've already seen it.
	var info URLInfo
	ukey := []byte(fmt.Sprintf("url/%s", link.URL.String()))
	if err := c.db.GetObj(ukey, &info); err == nil {
		return nil
	}

	// Store the URL in the queue, and store an empty URLInfo to
	// make sure that subsequent calls to Enqueue with the same
	// URL will fail.
	if err := c.addNewURLToQueue(link.URL, depth); err != nil {
		return err
	}

	return c.db.PutObj(ukey, &info)
}

func (c *Crawler) addNewURLToQueue(uri *url.URL, depth int) error {
	data, err := json.Marshal(&queueItem{
		URL:   uri.String(),
		Depth: depth,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()
	tag := []byte(uri.Host)
	return c.queue.Add(ctx, tag, data)
}

// Scan the queue for URLs until there are no more.
func (c *Crawler) worker(ctx context.Context) {
	for {
		job, err := c.queue.Next(ctx)
		if err == io.EOF || err == context.Canceled {
			return
		} else if err != nil {
			log.Printf("queue.Next() error: %v", err)
			return
		}

		if err := job.Done(ctx, c.handleJob(ctx, job)); err != nil {
			log.Printf("job.Done() error: %v", err)
		}
	}
}

func (c *Crawler) handleJob(ctx context.Context, job jq.Job) error {
	var item queueItem
	if err := json.Unmarshal(job.Data(), &item); err != nil {
		log.Printf("error decoding payload: %v", err)
		return nil // Permanent error.
	}

	return c.handleURL(ctx, &item)
}

func (c *Crawler) handleURL(ctx context.Context, item *queueItem) error {
	// Retrieve the URLInfo object from the crawl db.
	// Ignore errors, we can work with an empty object.
	urlkey := []byte(fmt.Sprintf("url/%s", item.URL))
	var info URLInfo
	c.db.GetObj(urlkey, &info) // nolint
	info.CrawledAt = time.Now()
	info.URL = item.URL

	// Fetch the URL and handle it. Make sure to Close the
	// response body (even if it gets replaced in the
	// Response object).
	fmt.Printf("%s\n", item.URL)
	httpResp, httpErr := c.fetcher.Fetch(item.URL)
	if httpErr == nil {
		defer httpResp.Body.Close() // nolint
		info.StatusCode = httpResp.StatusCode
	}

	// Invoke the handler (even if the fetcher errored
	// out). Errors in handling requests are fatal, crawl
	// will be aborted.
	err := c.handler.Handle(c, item.URL, item.Depth, httpResp, httpErr)

	switch err {
	case nil:
	case ErrRetryRequest:
		return err
	default:
		// Unexpected fatal error in handler.
		log.Fatalf("fatal error in handling %s: %v", item.URL, err)
	}

	// Write the result in our database.
	return c.db.PutObj(urlkey, &info)
}

// MustParseURLs parses a list of URLs and aborts on failure.
func MustParseURLs(urls []string) []*url.URL {
	// Parse the seed URLs.
	var parsed []*url.URL
	for _, s := range urls {
		u, err := url.Parse(s)
		if err != nil {
			log.Fatalf("error parsing URL \"%s\": %v", s, err)
		}
		parsed = append(parsed, u)
	}
	return parsed
}

// NewCrawler creates a new Crawler object with the specified behavior.
func NewCrawler(path string, seeds []*url.URL, scope Scope, f Fetcher, h Handler) (*Crawler, error) {
	// Open the crawl database.
	db, err := newGobDB(path)
	if err != nil {
		return nil, err
	}

	// Create the queue.
	q, err := queue.NewQueue(db.DB, queue.WithRetryInterval(ErrorRetryDelay))
	if err != nil {
		return nil, err
	}
	q.Start()

	// Create a context to control the workers.
	ctx, cancel := context.WithCancel(context.Background())

	c := &Crawler{
		db:          db,
		queue:       q.Client(),
		fetcher:     f,
		handler:     h,
		seeds:       seeds,
		scope:       scope,
		workerCtx:   ctx,
		stopWorkers: cancel,
	}

	return c, nil
}

// Run the crawl with the specified number of workers. This function
// does not exit until all work is done (no URLs left in the queue) or
// Stop is called.
func (c *Crawler) Run(concurrency int) {
	// Load initial seeds into the queue.
	for _, u := range c.seeds {
		Must(c.Enqueue(Outlink{URL: u, Tag: TagPrimary}, 0))
	}

	// Start some runners and wait until they're done.
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			c.worker(c.workerCtx)
			wg.Done()
		}()
	}
	wg.Wait()
}

// Stop a running crawl. This will cause a running Run function to return.
func (c *Crawler) Stop() {
	c.stopWorkers()
}

// Close the database and release resources associated with the crawler state.
func (c *Crawler) Close() {
	c.db.Close() // nolint
}

// FollowRedirects returns a Handler that follows HTTP redirects
// and adds them to the queue for crawling. It will call the wrapped
// handler on all requests regardless.
func FollowRedirects(wrap Handler) Handler {
	return HandlerFunc(func(c *Crawler, u string, depth int, resp *http.Response, err error) error {
		if herr := wrap.Handle(c, u, depth, resp, err); herr != nil {
			return herr
		}

		if err != nil {
			return nil
		}

		location := resp.Header.Get("Location")
		if resp.StatusCode >= 300 && resp.StatusCode < 400 && location != "" {
			locationURL, uerr := resp.Request.URL.Parse(location)
			if uerr != nil {
				log.Printf("error parsing Location header: %v", uerr)
			} else {
				return c.Enqueue(Outlink{URL: locationURL, Tag: TagPrimary}, depth+1)
			}
		}
		return nil
	})
}

// FilterErrors returns a Handler that forwards only requests with a
// "successful" HTTP status code (anything < 400). When using this
// wrapper, subsequent Handle calls will always have err set to nil.
func FilterErrors(wrap Handler) Handler {
	return HandlerFunc(func(c *Crawler, u string, depth int, resp *http.Response, err error) error {
		if err != nil {
			return nil
		}
		if resp.StatusCode >= 400 {
			return nil
		}
		return wrap.Handle(c, u, depth, resp, nil)
	})
}

// HandleRetries returns a Handler that will retry requests on
// temporary errors (all transport-level errors are considered
// temporary, as well as any HTTP status code >= 500).
func HandleRetries(wrap Handler) Handler {
	return HandlerFunc(func(c *Crawler, u string, depth int, resp *http.Response, err error) error {
		if err != nil || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return ErrRetryRequest
		}
		return wrap.Handle(c, u, depth, resp, nil)
	})
}

// Must will abort the program with a message when we encounter an
// error that we can't recover from.
func Must(err error) {
	if err != nil {
		log.Fatalf("fatal error: %v", err)
	}
}
