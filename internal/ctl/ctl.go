package ctl

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"text/tabwriter"
	"time"
)

type Client struct {
	Base string // http://host:port
	HTTP *http.Client
}

func New(base string) *Client {
	return &Client{Base: base, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

func (c *Client) get(path string, q url.Values, v any) error {
	u := c.Base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	resp, err := c.HTTP.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s: %s", resp.Status, msg)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

func (c *Client) Stats(out io.Writer) error {
	var s map[string]float64
	if err := c.get("/stats", nil, &s); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	for _, k := range []string{"ingested", "duplicates", "late", "emitted", "checkpoints", "inflight", "last_checkpoint_seconds", "p50_ms", "p99_ms"} {
		fmt.Fprintf(tw, "%s\t%g\n", k, s[k])
	}
	return tw.Flush()
}

func (c *Client) Top(out io.Writer, by string, n int) error {
	var body struct {
		Data []struct {
			Key        string
			Value      float64
			Start, End int64
		}
	}
	q := url.Values{"by": {by}, "n": {strconv.Itoa(n)}}
	if err := c.get("/top", q, &body); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintf(tw, "KEY\t%s\tWINDOW\n", by)
	for _, r := range body.Data {
		fmt.Fprintf(tw, "%s\t%g\t[%d,%d)\n", r.Key, r.Value, r.Start, r.End)
	}
	return tw.Flush()
}

func (c *Client) Results(out io.Writer, key string, limit int) error {
	var body struct {
		Data []struct {
			Key        string
			Start, End int64
			Count      int64
			Sum, Avg   float64
			Min, Max   float64
		}
	}
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if key != "" {
		q.Set("key", key)
	}
	if err := c.get("/results", q, &body); err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "KEY\tWINDOW\tCOUNT\tSUM\tAVG\tMIN\tMAX")
	for _, r := range body.Data {
		fmt.Fprintf(tw, "%s\t[%d,%d)\t%d\t%g\t%.4g\t%g\t%g\n", r.Key, r.Start, r.End, r.Count, r.Sum, r.Avg, r.Min, r.Max)
	}
	return tw.Flush()
}

func (c *Client) Ready() error {
	resp, err := c.HTTP.Get(c.Base + "/readyz")
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("not ready: %s", resp.Status)
	}
	return nil
}
