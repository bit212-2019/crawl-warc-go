// A restartable crawler that extracts links from HTML pages and
// simply prints them.
//

package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	"git.autistici.org/ale/crawl"
	"git.autistici.org/ale/crawl/analysis"
)

var (
	concurrency  = flag.Int("c", 10, "concurrent workers")
	depth        = flag.Int("depth", 10, "maximum link depth")
	validSchemes = flag.String("schemes", "http,https", "comma-separated list of allowed protocols")
)

func extractLinks(c *crawl.Crawler, u string, depth int, resp *http.Response, _ error) error {
	links, err := analysis.GetLinks(resp)
	if err != nil {
		// Not a fatal error, just a bad web page.
		return nil
	}

	for _, link := range links {
		if err := c.Enqueue(link, depth+1); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	flag.Parse()

	seeds := crawl.MustParseURLs(flag.Args())
	scope := crawl.AND(
		crawl.NewSchemeScope(strings.Split(*validSchemes, ",")),
		crawl.NewDepthScope(*depth),
		crawl.NewSeedScope(seeds),
	)

	crawler, err := crawl.NewCrawler(
		"crawldb",
		seeds,
		scope,
		crawl.FetcherFunc(http.Get),
		crawl.HandleRetries(crawl.FollowRedirects(crawl.FilterErrors(crawl.HandlerFunc(extractLinks)))),
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}
	crawler.Run(*concurrency)
	crawler.Close()
}
