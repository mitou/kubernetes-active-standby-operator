package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ProbeResult is the outcome of a single health probe against a pod.
type ProbeResult struct {
	OK      bool
	Latency time.Duration
	Status  int
	Err     error
}

// probePod issues an HTTP GET health probe to http://ip:port/path. A pod is
// healthy when it answers with a 2xx status within the timeout. If expectBody is
// non-empty, the response body must also contain that substring — this catches
// endpoints that return 200 while signalling trouble in the body (for example an
// app that reports a degraded dependency without changing its status code).
func probePod(ctx context.Context, ip string, port int, path, expectBody string, timeout time.Duration) ProbeResult {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := fmt.Sprintf("http://%s:%d%s", ip, port, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ProbeResult{Latency: time.Since(start), Err: err}
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ProbeResult{Latency: time.Since(start), Err: err}
	}
	defer resp.Body.Close()

	latency := time.Since(start)
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ProbeResult{Latency: latency, Status: resp.StatusCode,
			Err: fmt.Errorf("unhealthy status code %d", resp.StatusCode)}
	}
	if expectBody != "" && !strings.Contains(string(body), expectBody) {
		return ProbeResult{Latency: latency, Status: resp.StatusCode,
			Err: fmt.Errorf("response body did not contain %q", expectBody)}
	}

	return ProbeResult{OK: true, Latency: latency, Status: resp.StatusCode}
}
