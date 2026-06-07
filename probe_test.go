package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

func hostPort(t *testing.T, raw string) (string, int) {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	host, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split %q: %v", u.Host, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("port %q: %v", portStr, err)
	}
	return host, port
}

func TestProbePod_2xxIsHealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)

	res := probePod(context.Background(), host, port, "/healthz", "", time.Second)
	if !res.OK {
		t.Fatalf("expected OK, got err=%v status=%d", res.Err, res.Status)
	}
}

func TestProbePod_Non2xxIsUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)

	res := probePod(context.Background(), host, port, "/healthz", "", time.Second)
	if res.OK {
		t.Fatalf("expected unhealthy for 500")
	}
	if res.Status != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", res.Status)
	}
}

func TestProbePod_ExpectBodyMatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"OK"}`))
	}))
	defer srv.Close()
	host, port := hostPort(t, srv.URL)

	if res := probePod(context.Background(), host, port, "/healthz", `"status":"OK"`, time.Second); !res.OK {
		t.Fatalf("expected OK when body contains the expected substring, got err=%v", res.Err)
	}
	if res := probePod(context.Background(), host, port, "/healthz", "UNHEALTHY", time.Second); res.OK {
		t.Fatalf("expected unhealthy when body lacks the expected substring")
	}
}

func TestProbePod_ConnectionRefusedIsUnhealthy(t *testing.T) {
	// Port 1 is reserved and not listening, so the dial fails fast.
	res := probePod(context.Background(), "127.0.0.1", 1, "/healthz", "", 500*time.Millisecond)
	if res.OK {
		t.Fatalf("expected unhealthy when the pod is unreachable")
	}
	if res.Err == nil {
		t.Fatalf("expected an error to be recorded")
	}
}
