package main

import (
	"context"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	"crane/internal/crane/control"
	"crane/internal/crane/protocol"
	"crane/internal/wire"

	"crane/internal/clock"
	"crane/internal/config"
)

//go:embed dashboard.html
var dashboardPage []byte

// jobListFetcher is the read-only control surface the dashboard serves.
type jobListFetcher interface {
	ListJobs(ctx context.Context) (protocol.JobListResponse, error)
}

// validateDashboardAddress accepts only explicit loopback host:port addresses.
func validateDashboardAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return errors.New("-dashboard requires a loopback host:port address")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 || strconv.FormatUint(port, 10) != portText {
		return errors.New("-dashboard port must be 1 through 65535")
	}
	if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
		if host != "localhost" {
			return errors.New("-dashboard address must be loopback")
		}
	}
	return nil
}

// dashboardStateName mirrors the CLI's public lifecycle names.
func dashboardStateName(state protocol.JobState) string {
	switch state {
	case protocol.JobPending:
		return "pending"
	case protocol.JobDeploying:
		return "deploying"
	case protocol.JobRunning:
		return "running"
	case protocol.JobDraining:
		return "draining"
	case protocol.JobSucceeded:
		return "succeeded"
	case protocol.JobFailed:
		return "failed"
	case protocol.JobCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

func dashboardJobJSON(status protocol.StatusResponse) map[string]any {
	output := map[string]any{
		"job_id":                 hex.EncodeToString(status.JobID[:]),
		"state":                  dashboardStateName(status.State),
		"state_number":           uint8(status.State),
		"job_control_revision":   status.JobControlRevision,
		"topology_digest":        hex.EncodeToString(status.TopologyDigest[:]),
		"source_task_count":      status.SourceTaskCount,
		"completed_source_tasks": status.CompletedSourceTasks,
		"result_partition_count": status.ResultPartitionCount,
		"manifest_count":         status.ManifestCount,
		"manifest_set_digest":    "",
		"assignment_revision":    status.AssignmentRevision,
		"has_failure":            status.HasFailure,
	}
	if status.HasManifestSet {
		output["manifest_set_digest"] = hex.EncodeToString(status.ManifestSetDigest[:])
	}
	if status.HasFailure {
		output["failure_code"] = uint16(status.FailureCode)
	}
	return output
}

// newDashboardMux serves the dashboard page and its single JSON endpoint.
func newDashboardMux(clusterID string, fetcher jobListFetcher) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/jobs", func(writer http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), 2*time.Second)
		defer cancel()
		listing, err := fetcher.ListJobs(ctx)
		writer.Header().Set("Content-Type", "application/json")
		if err != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(writer).Encode(map[string]any{"error": err.Error()})
			return
		}
		jobs := make([]map[string]any, 0, len(listing.Jobs))
		for _, status := range listing.Jobs {
			jobs = append(jobs, dashboardJobJSON(status))
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"cluster_id": clusterID, "leader_node_id": listing.LeaderNodeID, "applied_index": listing.AppliedIndex, "jobs": jobs,
		})
	})
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = writer.Write(dashboardPage)
	})
	return mux
}

// startDashboard serves the dashboard until the context ends or Stop runs.
func startDashboard(ctx context.Context, clusterID, address string, fetcher jobListFetcher, errorsOut io.Writer) (string, func(), error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return "", nil, fmt.Errorf("bind dashboard %s: %w", address, err)
	}
	server := &http.Server{Handler: newDashboardMux(clusterID, fetcher), ReadHeaderTimeout: 5 * time.Second}
	logger := log.New(errorsOut, "dashboard: ", 0)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Print(serveErr)
		}
	}()
	stop := func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		<-serveDone
	}
	go func() {
		<-ctx.Done()
		stop()
	}()
	return "http://" + listener.Addr().String(), stop, nil
}

// newDashboardFetcher composes a read-only control client for the cluster the
// launcher is about to run.
func newDashboardFetcher(configuration config.NodeConfig, secretFile string) (jobListFetcher, error) {
	secret, err := config.LoadClusterSecret(secretFile)
	if err != nil {
		return nil, err
	}
	return control.NewClient(control.ClientOptions{
		Config:        configuration,
		Authenticator: wire.NewHMACAuthenticator(secret),
		Clock:         clock.NewReal(),
	})
}
