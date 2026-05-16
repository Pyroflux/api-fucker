package main

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"api-fucker/internal/store"
)

func TestIPAllowedMatchesExactAndWildcard(t *testing.T) {
	if !ipAllowed("127.0.0.1", []string{"127.0.0.1"}, nil) {
		t.Fatal("exact allowlist should allow")
	}
	if !ipAllowed("192.168.1.8", []string{"192.*"}, nil) {
		t.Fatal("wildcard allowlist should allow")
	}
	if ipAllowed("10.0.0.1", []string{"192.*"}, nil) {
		t.Fatal("non-matching allowlist should block")
	}
}

func TestIPAllowedBlocklistWins(t *testing.T) {
	if ipAllowed("127.0.0.1", []string{"127.*"}, []string{"127.0.0.1"}) {
		t.Fatal("blocklist should win over allowlist")
	}
}

func TestIPAccessControlBlocksAndAllowsV1Only(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SaveGlobalConfig(store.GlobalConfig{
		UpstreamBaseURL: "https://upstream.example",
		IPAllowlist:     []string{"127.*"},
		IPBlocklist:     []string{"127.0.0.2"},
	}); err != nil {
		t.Fatal(err)
	}
	handler := ipAccessControl(st, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	allowed := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	allowed.RemoteAddr = "127.0.0.1:12345"
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, allowed)
	if res.Code != http.StatusOK {
		t.Fatalf("allowed status = %d", res.Code)
	}

	blocked := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	blocked.RemoteAddr = "127.0.0.2:12345"
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, blocked)
	if res.Code != http.StatusForbidden {
		t.Fatalf("blocked status = %d", res.Code)
	}
	if got := res.Body.String(); got != "your ip 127.0.0.2 is not allowed\n" {
		t.Fatalf("blocked body = %q", got)
	}

	healthz := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthz.RemoteAddr = "127.0.0.2:12345"
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, healthz)
	if res.Code != http.StatusOK {
		t.Fatalf("healthz status = %d", res.Code)
	}
}
