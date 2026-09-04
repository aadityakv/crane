package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"crane/internal/crane/protocol"
)

type stubFetcher struct {
	listing protocol.JobListResponse
	err     error
}

func (stub *stubFetcher) ListJobs(context.Context) (protocol.JobListResponse, error) {
	return stub.listing, stub.err
}

func TestValidateDashboardAddressAcceptsOnlyLoopback(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8080", "localhost:9000", "[::1]:8080"} {
		if err := validateDashboardAddress(address); err != nil {
			t.Fatalf("address %q rejected: %v", address, err)
		}
	}
	for _, address := range []string{"0.0.0.0:8080", "example.com:80", "127.0.0.1", "127.0.0.1:0", "127.0.0.1:99999", ":8080"} {
		if err := validateDashboardAddress(address); err == nil {
			t.Fatalf("address %q accepted", address)
		}
	}
}

func TestDashboardJobsEndpointMapsSuccessAndFailure(t *testing.T) {
	mux := newDashboardMux("6ba7b810-9dad-41d1-80b4-00c04fd430c8", &stubFetcher{listing: protocol.JobListResponse{LeaderNodeID: 2, AppliedIndex: 9}})

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	body, _ := io.ReadAll(response.Body)
	for _, want := range []string{`"leader_node_id":2`, `"jobs":[]`, `"cluster_id":"6ba7b810-9dad-41d1-80b4-00c04fd430c8"`} {
		if !strings.Contains(string(body), want) {
			t.Fatalf("body %s lacks %s", body, want)
		}
	}

	failing := newDashboardMux("6ba7b810-9dad-41d1-80b4-00c04fd430c8", &stubFetcher{err: errors.New("leader unavailable")})
	rejected := httptest.NewRecorder()
	failing.ServeHTTP(rejected, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))
	if rejected.Code != http.StatusServiceUnavailable {
		t.Fatalf("failure status = %d, want 503", rejected.Code)
	}
	if !strings.Contains(rejected.Body.String(), "leader unavailable") {
		t.Fatalf("failure body = %s", rejected.Body.String())
	}
}

func TestDashboardPageServesEmbeddedAssetWithoutExternalReferences(t *testing.T) {
	mux := newDashboardMux("6ba7b810-9dad-41d1-80b4-00c04fd430c8", &stubFetcher{})
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("page response = %d %s", response.Code, response.Header().Get("Content-Type"))
	}
	page := string(dashboardPage)
	for _, banned := range []string{`src="http`, `href="http`, "https://"} {
		if strings.Contains(page, banned) {
			t.Fatalf("page references an external resource via %q", banned)
		}
	}
}

func TestStartDashboardServesUntilStopped(t *testing.T) {
	url, stop, err := startDashboard(context.Background(), "6ba7b810-9dad-41d1-80b4-00c04fd430c8", "127.0.0.1:0", &stubFetcher{}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(url + "/api/jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
}
