package crawl

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestCrawler(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// Run a trivial test http server just so our test Fetcher can
	// return a real http.Response object.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "hello")
	}))
	defer srv.Close()

	seeds := MustParseURLs([]string{srv.URL})
	scope := AND(
		NewSchemeScope([]string{"http"}),
		NewSeedScope(seeds),
		NewDepthScope(2),
	)

	var crawledPages int
	h := HandlerFunc(func(c *Crawler, u string, depth int, resp *http.Response, err error) error {
		crawledPages++
		next := fmt.Sprintf(srv.URL+"/page/%d", crawledPages)
		log.Printf("%s -> %s", u, next)
		c.Enqueue(Outlink{
			URL: mustParseURL(next),
			Tag: TagPrimary,
		}, depth+1)
		return nil
	})

	crawler, err := NewCrawler(dir+"/crawl.db", seeds, scope, FetcherFunc(http.Get), HandleRetries(FilterErrors(FollowRedirects(h))), nil)
	if err != nil {
		t.Fatal("NewCrawler", err)
	}

	crawler.Run(1)
	crawler.Close()

	if crawledPages != 2 {
		t.Fatalf("incomplete/bad crawl (%d pages, expected %d)", crawledPages, 10)
	}
}
