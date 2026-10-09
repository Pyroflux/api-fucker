package store_test

import (
	"api-fucker/internal/admin"
	"api-fucker/internal/gateway"
	"api-fucker/internal/store"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestForwardingAndHeartbeatWhileWriterBlocked(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := store.SeedRuntimeForTest(st, 7300); err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"ok":true}`)) }))
	defer upstream.Close()
	if err := st.SaveGlobalConfig(store.GlobalConfig{UpstreamBaseURL: upstream.URL, GlobalMaxConcurrency: 1, ProxyPoolEnabled: true, ProxyMode: store.ProxyModeRoundRobin}); err != nil {
		t.Fatal(err)
	}
	// A mock HTTP proxy returns the upstream response without external traffic.
	gw := gateway.New(st, nil)
	adm := admin.New(st, "admin-test", gw.CurrentConcurrency, gw)
	adm.SetProxyNodeToken("node-test")
	if _, err := st.UpsertProxyNodeReport(store.ProxyNodeReport{ID: "node", ProxyURL: upstream.URL, MaxConcurrency: 1}, "127.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	release, err := store.HoldWriterForTest(st)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := st.RecordMetric("key-00000", store.MetricSample{Time: time.Now()}); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	flushed := make(chan error, 1)
	go func() { flushed <- st.Flush() }()
	// The pending flush forces the real writer to wait on the held transaction.
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-flushed:
		t.Fatalf("flush should be blocked: %v", err)
	default:
	}
	var wg sync.WaitGroup
	latencies := make([]time.Duration, 512)
	for i := range latencies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			begin := time.Now()
			if err := st.RecordMetric("key-00000", store.MetricSample{Time: time.Now()}); err != nil {
				t.Error(err)
			}
			latencies[i] = time.Since(begin)
		}(i)
	}
	wg.Wait()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if latencies[len(latencies)-1] > 500*time.Millisecond {
		t.Fatalf("metric update blocked: %s", latencies[len(latencies)-1])
	}
	t.Logf("512 concurrent metric updates, 7300 keys + 7-day history: p95=%s p99=%s max=%s", latencies[486], latencies[506], latencies[511])
	var forwardTimes []time.Duration
	check := func() {
		begin := time.Now()
		report := httptest.NewRequest(http.MethodPost, "/api/admin/proxies/report", strings.NewReader(fmt.Sprintf(`{"id":"node","proxyUrl":%q,"maxConcurrency":1}`, upstream.URL)))
		report.Header.Set("Authorization", "Bearer node-test")
		rec := httptest.NewRecorder()
		adm.ServeHTTP(rec, report)
		if rec.Code != http.StatusOK {
			t.Fatalf("heartbeat: %d %s", rec.Code, rec.Body.String())
		}
		if elapsed := time.Since(begin); elapsed > 500*time.Millisecond {
			t.Fatalf("heartbeat blocked: %s", elapsed)
		} else {
			t.Logf("heartbeat: %s", elapsed)
		}
		for i := 0; i < 32; i++ {
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"test","messages":[]}`))
			rec = httptest.NewRecorder()
			begin = time.Now()
			gw.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("forward/new admission: %d %s", rec.Code, rec.Body.String())
			}
			if elapsed := time.Since(begin); elapsed > 500*time.Millisecond {
				t.Fatalf("forward blocked: %s", elapsed)
			} else {
				forwardTimes = append(forwardTimes, elapsed)
			}
			for id, count := range gw.CurrentConcurrency() {
				if count != 0 {
					t.Fatalf("slot leaked %s: %d", id, count)
				}
			}
		}
	}
	check()
	registerStart := time.Now()
	if _, err := st.UpsertProxyNodeReport(store.ProxyNodeReport{ID: "brand-new", ProxyURL: upstream.URL}, "127.0.0.1:9000"); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(registerStart); elapsed > 500*time.Millisecond {
		t.Fatalf("first registration blocked: %s", elapsed)
	}
	// Keep the actual write lock for 15 s, exceeding the proxy's 10 s deadline.
	if remaining := 15*time.Second - time.Since(started); remaining > 0 {
		time.Sleep(remaining)
	}
	check()
	sort.Slice(forwardTimes, func(i, j int) bool { return forwardTimes[i] < forwardTimes[j] })
	t.Logf("forwarding + completion under blocked writer: p95=%s p99=%s max=%s", forwardTimes[60], forwardTimes[63], forwardTimes[63])
	release()
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not recover")
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf("heap in use after blocked-writer scenario: %.1f MiB; persistence: %+v", float64(memory.HeapAlloc)/(1024*1024), st.PersistenceHealth())
}
