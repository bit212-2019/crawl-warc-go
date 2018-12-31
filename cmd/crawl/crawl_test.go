package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"git.autistici.org/ale/crawl"
	"git.autistici.org/ale/crawl/warc"
)

func linkTo(w http.ResponseWriter, uri string) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, "<html><body><a href=\"%s\">link!</a></body></html>", uri)
}

func TestCrawl(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			linkTo(w, "/redir")
		case "/b":
			linkTo(w, "/")
		case "/redir":
			http.Redirect(w, r, "/b", http.StatusFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	seeds := crawl.MustParseURLs([]string{srv.URL + "/"})
	scope := crawl.AND(
		crawl.NewSchemeScope([]string{"http"}),
		crawl.NewDepthScope(10),
		crawl.NewSeedScope(seeds),
	)

	outf, err := os.Create(filepath.Join(tmpdir, "warc.gz"))
	if err != nil {
		t.Fatal(err)
	}
	w := warc.NewWriter(outf)
	defer w.Close()
	saver, err := newWarcSaveHandler(w)
	if err != nil {
		t.Fatal(err)
	}

	crawler, err := crawl.NewCrawler(
		filepath.Join(tmpdir, "db"),
		seeds,
		scope,
		crawl.FetcherFunc(fetch),
		crawl.HandleRetries(crawl.FollowRedirects(crawl.FilterErrors(saver))),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	crawler.Run(1)
	crawler.Close()

	if n := saver.(*warcSaveHandler).numWritten; n != 3 {
		t.Fatalf("warc handler wrote %d records, expected 3", n)
	}
}
