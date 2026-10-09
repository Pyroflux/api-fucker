package main

import (
	"bytes"
	"errors"
	"log"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestReportLogRateLimitRecoveryAndRedaction(t *testing.T) {
	var output bytes.Buffer
	original := log.Writer()
	log.SetOutput(&output)
	defer log.SetOutput(original)
	ps := &proxyServer{token: "node-secret"}
	err := &url.Error{Op: "Post", URL: "http://user:pass@example.invalid/report?token=node-secret", Err: errors.New("context deadline exceeded")}
	ps.recordReportResult(err, 10*time.Second, "node-secret")
	ps.recordReportResult(err, 10*time.Second, "node-secret")
	if strings.Count(output.String(), "failed after") != 1 {
		t.Fatalf("duplicate logs: %s", output.String())
	}
	for _, secret := range []string{"user:pass", "node-secret", "?token="} {
		if strings.Contains(output.String(), secret) || strings.Contains(ps.lastErrorValue(), secret) {
			t.Fatalf("credential not redacted")
		}
	}
	if !strings.Contains(ps.lastErrorValue(), "context deadline exceeded") {
		t.Fatal("error cause lost")
	}
	ps.recordReportResult(nil, time.Millisecond, "node-secret")
	if !strings.Contains(output.String(), "recovered") || ps.lastErrorValue() != "" {
		t.Fatal("report recovery was not recorded")
	}
}

func TestForwardFailureSurvivesSuccessfulHeartbeat(t *testing.T) {
	ps := &proxyServer{}
	ps.recordFailure(errors.New("upstream unavailable"))
	ps.recordReportResult(nil, time.Millisecond, "")
	if ps.lastErrorValue() != "upstream unavailable" || ps.failed.Load() != 1 {
		t.Fatal("heartbeat erased forwarding error")
	}
}
