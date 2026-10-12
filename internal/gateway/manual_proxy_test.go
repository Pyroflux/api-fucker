package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"api-fucker/internal/store"
)

const manualProxyA = "http://a.example:8080"
const manualProxyB = "http://b.example:8080"
const manualProxyC = "https://c.example:8443"

func manualProxyConfig() store.GlobalConfig {
	return store.GlobalConfig{UpstreamBaseURL: "https://upstream.example", DefaultProxyURL: manualProxyA + "\n" + manualProxyB + "\n" + manualProxyC, ProxyMode: store.ProxyModeRoundRobin}
}

func TestManualProxyRoundRobinAndConfigChanges(t *testing.T) {
	g := New(nil, nil)
	cfg := manualProxyConfig()
	key := store.Key{ID: "key-a"}
	choose := func(want string) {
		t.Helper()
		selected, err := g.selectedForKey(key, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if selected.proxyURL != want {
			t.Fatalf("proxy=%q want=%q", selected.proxyURL, want)
		}
	}
	choose(manualProxyA)
	choose(manualProxyB)
	choose(manualProxyC)
	choose(manualProxyA)
	key.ProxyURL = "http://override.example:8080"
	choose(key.ProxyURL)
	key.ProxyURL = ""
	choose(manualProxyB)
	cfg.DefaultProxyURL = manualProxyB + "\n" + manualProxyA
	choose(manualProxyB)
	cfg.ProxyMode = store.ProxyModeKeyBinding
	binding, err := g.selectedForKey(key, cfg)
	if err != nil {
		t.Fatal(err)
	}
	choose(binding.proxyURL)
	choose(binding.proxyURL)
	cfg.ProxyMode = store.ProxyModeRoundRobin
	choose(manualProxyB)
	cfg.DefaultProxyURL = ""
	choose("")
	cfg.DefaultProxyURL = manualProxyA
	choose(manualProxyA)
	choose(manualProxyA)
	cfg.DefaultProxyURL = "http://a.example:8080\ninvalid"
	if _, err := g.selectedForKey(key, cfg); err == nil {
		t.Fatal("invalid cached list fell back to direct access")
	}
	cfg.DefaultProxyURL = manualProxyC
	choose(manualProxyC)
}

func TestManualProxyKeyBindingStableAcrossRequestsAndRestart(t *testing.T) {
	cfg := manualProxyConfig()
	cfg.ProxyMode = store.ProxyModeKeyBinding
	g := New(nil, nil)
	bindings := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := store.Key{ID: fmt.Sprintf("key-%d", i)}
		selected, err := g.selectedForKey(key, cfg)
		if err != nil {
			t.Fatal(err)
		}
		bindings[key.ID] = selected.proxyURL
	}
	for _, gateway := range []*Gateway{g, New(nil, nil)} {
		for repeat := 0; repeat < 3; repeat++ {
			for id, want := range bindings {
				selected, err := gateway.selectedForKey(store.Key{ID: id}, cfg)
				if err != nil {
					t.Fatal(err)
				}
				if selected.proxyURL != want {
					t.Fatal("key binding changed with request order or gateway restart")
				}
			}
		}
	}
	cfg.DefaultProxyURL = manualProxyB
	for id := range bindings {
		selected, err := g.selectedForKey(store.Key{ID: id}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if selected.proxyURL != manualProxyB {
			t.Fatal("removed proxy remained bound")
		}
	}
}

func TestManualProxyConcurrentRoundRobin(t *testing.T) {
	g := New(nil, nil)
	cfg := manualProxyConfig()
	const requests = 600
	results := make(chan string, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			selected, err := g.selectedForKey(store.Key{ID: "key"}, cfg)
			if err != nil {
				t.Error(err)
				return
			}
			results <- selected.proxyURL
		}()
	}
	wg.Wait()
	close(results)
	counts := make(map[string]int)
	for proxy := range results {
		counts[proxy]++
	}
	for _, proxy := range []string{manualProxyA, manualProxyB, manualProxyC} {
		if counts[proxy] != requests/3 {
			t.Fatalf("unbalanced rotation: %+v", counts)
		}
	}
}

func TestManualProxyRejectedAdminTestDoesNotAdvanceSelection(t *testing.T) {
	g := New(nil, nil)
	cfg := manualProxyConfig()
	key := store.Key{ID: "key"}
	if _, err := g.TestKey(context.Background(), key, cfg, " ", "ping"); err == nil {
		t.Fatal("empty model accepted")
	}
	selected, err := g.selectedForKey(key, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if selected.proxyURL != manualProxyA {
		t.Fatal("invalid admin input consumed manual proxy cursor")
	}
}

func TestManualProxyForwardingRetriesAdminAndClientReuse(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream_%v", stream), func(t *testing.T) {
			st, err := store.Open(t.TempDir() + "/data.db")
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			cfg := manualProxyConfig()
			cfg.DefaultProxyURL = manualProxyA + "\n" + manualProxyB
			if err := st.SaveGlobalConfig(cfg); err != nil {
				t.Fatal(err)
			}
			key, err := st.CreateKey(store.Key{Name: "key", APIKey: "test-placeholder"})
			if err != nil {
				t.Fatal(err)
			}
			g := New(st, nil, "must-not-be-injected")
			var chosen []string
			creations := make(map[string]int)
			g.newClient = func(proxy string) (*http.Client, error) {
				creations[proxy]++
				return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					chosen = append(chosen, proxy)
					if len(chosen) == 1 {
						return nil, io.ErrUnexpectedEOF
					}
					return jsonResponse(r, http.StatusOK, map[string]any{"data": []map[string]string{{"id": "test-model"}}}), nil
				})}, nil
			}
			for _, path := range []string{chatPath, imageGenerationsPath, modelsPath} {
				method := http.MethodPost
				if path == modelsPath {
					method = http.MethodGet
				}
				req := httptest.NewRequest(method, path, strings.NewReader(fmt.Sprintf(`{"model":"test","stream":%v}`, stream)))
				res := httptest.NewRecorder()
				g.ServeHTTP(res, req)
				if res.Code != http.StatusOK {
					t.Fatalf("forward: status=%d", res.Code)
				}
			}
			// Retain the existing admin bypass of an enabled VPS pool, even with no nodes.
			cfg.ProxyPoolEnabled = true
			if _, err := g.TestKey(context.Background(), key, cfg, "test-model", "ping"); err != nil {
				t.Fatal(err)
			}
			if _, err := g.ListModelsForKey(context.Background(), key, cfg); err != nil {
				t.Fatal(err)
			}
			want := []string{manualProxyA, manualProxyA, manualProxyB, manualProxyA, manualProxyB, manualProxyA}
			if !reflect.DeepEqual(chosen, want) {
				t.Fatalf("actual request proxies=%v want=%v", chosen, want)
			}
			if creations[manualProxyA] != 1 || creations[manualProxyB] != 1 || len(creations) != 2 {
				t.Fatalf("HTTP clients not reused: %v", creations)
			}
		})
	}
}

func TestManualProxyPriorityAndIndependentVPSCursor(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := manualProxyConfig()
	if err := st.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	key, err := st.CreateKey(store.Key{Name: "key", APIKey: "test-placeholder"})
	if err != nil {
		t.Fatal(err)
	}
	g := New(st, nil, "node-test-token")
	choose := func() selectedKey {
		t.Helper()
		selected, err := g.acquireKey()
		if err != nil {
			t.Fatal(err)
		}
		g.releaseSelected(selected)
		return selected
	}
	if got := choose(); got.proxyURL != manualProxyA || got.proxyNodeID != "" {
		t.Fatal("incorrect first manual selection")
	}
	key.ProxyURL = "http://test-user:test-password@override.example:8080"
	if _, err := st.UpdateKey(key.ID, key); err != nil {
		t.Fatal(err)
	}
	if got := choose(); got.proxyURL != key.ProxyURL {
		t.Fatal("key override/authentication changed")
	}
	cfg.ProxyPoolEnabled = true
	if err := st.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := g.acquireKey(); err != errNoAvailableProxy {
		t.Fatalf("VPS failure fell back to manual: %v", err)
	}
	for _, id := range []string{"node-a", "node-b"} {
		if _, err := st.UpsertProxyNodeReport(store.ProxyNodeReport{ID: id, ProxyURL: "http://" + id + ".example:9070"}, "127.0.0.1:1"); err != nil {
			t.Fatal(err)
		}
	}
	if got := choose(); got.proxyNodeID != "node-a" || !strings.Contains(got.proxyURL, "node-test-token") {
		t.Fatal("VPS priority/authentication changed")
	}
	cfg.ProxyPoolEnabled = false
	if err := st.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	key.ProxyURL = ""
	if _, err := st.UpdateKey(key.ID, key); err != nil {
		t.Fatal(err)
	}
	if got := choose(); got.proxyURL != manualProxyB {
		t.Fatal("VPS or key override consumed manual cursor")
	}
	cfg.ProxyPoolEnabled = true
	if err := st.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if got := choose(); got.proxyNodeID != "node-b" {
		t.Fatal("manual selections changed VPS cursor")
	}
	cfg.ProxyPoolEnabled = false
	cfg.DefaultProxyURL = "http://test-user:test-password@manual.example:8080"
	if err := st.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if got := choose(); got.proxyURL != cfg.DefaultProxyURL {
		t.Fatal("manual proxy authentication was overwritten by node token")
	}
	for _, count := range g.CurrentConcurrency() {
		if count != 0 {
			t.Fatal("key admission leaked")
		}
	}
}

func TestManualProxyConfigChangeDoesNotMoveInFlightRequest(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := manualProxyConfig()
	if err := st.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateKey(store.Key{Name: "key", APIKey: "test-placeholder", MaxConcurrency: 2}); err != nil {
		t.Fatal(err)
	}
	g := New(st, nil)
	started := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var mu sync.Mutex
	var chosen []string
	g.newClient = func(proxy string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			mu.Lock()
			chosen = append(chosen, proxy)
			mu.Unlock()
			if proxy == manualProxyA {
				close(started)
				select {
				case <-release:
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			}
			return jsonResponse(r, http.StatusOK, map[string]bool{"ok": true}), nil
		})}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first := httptest.NewRequest(http.MethodPost, chatPath, strings.NewReader(`{"model":"test"}`)).WithContext(ctx)
	done := make(chan int, 1)
	go func() { res := httptest.NewRecorder(); g.ServeHTTP(res, first); done <- res.Code }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first request did not reach its proxy")
	}
	cfg.DefaultProxyURL = manualProxyC
	if err := st.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	res := httptest.NewRecorder()
	g.ServeHTTP(res, httptest.NewRequest(http.MethodPost, chatPath, strings.NewReader(`{"model":"test"}`)))
	if res.Code != http.StatusOK {
		t.Fatalf("new configuration failed: %d", res.Code)
	}
	unblock()
	select {
	case code := <-done:
		if code != http.StatusOK {
			t.Fatalf("in-flight response: %d", code)
		}
	case <-ctx.Done():
		t.Fatal("in-flight request blocked")
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(chosen, []string{manualProxyA, manualProxyC}) {
		t.Fatalf("proxy moved during request: %v", chosen)
	}
}

func BenchmarkManualProxySelection(b *testing.B) {
	for _, size := range []int{1, 10, 1000} {
		for _, resave := range []bool{false, true} {
			b.Run(fmt.Sprintf("size_%d/resave_%v", size, resave), func(b *testing.B) {
				st, err := store.Open(b.TempDir() + "/data.db")
				if err != nil {
					b.Fatal(err)
				}
				defer st.Close()
				cfg := manualProxyConfig()
				urls := make([]string, size)
				for i := range urls {
					urls[i] = fmt.Sprintf("http://proxy-%d.example:8080", i)
				}
				cfg.DefaultProxyURL = strings.Join(urls, "\n")
				if err := st.SaveGlobalConfig(cfg); err != nil {
					b.Fatal(err)
				}
				cfg, _ = st.GlobalConfig()
				g := New(st, nil)
				key := store.Key{ID: "test-key"}
				if _, err := g.selectedForKey(key, cfg); err != nil {
					b.Fatal(err)
				}
				if resave {
					cfg.GlobalMaxConcurrency = 10
					if err := st.SaveGlobalConfig(cfg); err != nil {
						b.Fatal(err)
					}
					cfg, _ = st.GlobalConfig()
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := g.selectedForKey(key, cfg); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
			})
		}
	}
}
