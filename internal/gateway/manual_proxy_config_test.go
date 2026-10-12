package gateway

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"api-fucker/internal/store"
)

func TestManualProxyStaleAdminConfigUsesActiveRotation(t *testing.T) {
	for _, change := range []string{"list", "mode"} {
		t.Run(change, func(t *testing.T) {
			st, err := store.Open(t.TempDir() + "/data.db")
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			cfg := manualProxyConfig()
			if change == "mode" {
				cfg.ProxyMode = store.ProxyModeKeyBinding
			}
			if err := st.SaveGlobalConfig(cfg); err != nil {
				t.Fatal(err)
			}
			old, _ := st.GlobalConfig()
			key, err := st.CreateKey(store.Key{Name: "key", APIKey: "test-placeholder"})
			if err != nil {
				t.Fatal(err)
			}
			g := New(st, nil)
			var adminProxy string
			g.newClient = func(proxy string) (*http.Client, error) {
				return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					adminProxy = proxy
					return jsonResponse(r, http.StatusOK, map[string]bool{"ok": true}), nil
				})}, nil
			}
			// An admin handler has read its config, but its test goroutine has not
			// selected a proxy yet. Let new forwarding activate another config.
			prepared, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var adminErr error
			go func() {
				defer close(done)
				_, adminErr = g.TestKeyWithProgress(ctx, key, old, "test-model", "ping", func(event TestProgressEvent) {
					if event.Step == "prepare" {
						close(prepared)
						select {
						case <-resume:
						case <-ctx.Done():
						}
					}
				})
			}()
			defer func() { unblock(); cancel(); <-done }()
			select {
			case <-prepared:
			case <-ctx.Done():
				t.Fatal("admin request did not reach preparation")
			}
			cfg.ProxyMode = store.ProxyModeRoundRobin
			wantFirst, wantAdmin, wantNext := manualProxyA, manualProxyB, manualProxyC
			if change == "list" {
				cfg.DefaultProxyURL = manualProxyB + "\n" + manualProxyC
				wantFirst, wantAdmin, wantNext = manualProxyB, manualProxyC, manualProxyB
			}
			if err := st.SaveGlobalConfig(cfg); err != nil {
				t.Fatal(err)
			}
			choose := func(want string) {
				t.Helper()
				selected, err := g.acquireKey()
				if err != nil {
					t.Fatal(err)
				}
				g.releaseSelected(selected)
				if selected.proxyURL != want {
					t.Fatalf("forwarding proxy = %s, want %s", selected.proxyURL, want)
				}
			}
			choose(wantFirst)
			unblock()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("admin request did not complete")
			}
			if adminErr != nil {
				t.Fatal(adminErr)
			}
			if adminProxy != wantAdmin {
				t.Fatalf("stale admin config rolled back active rotation: proxy = %s, want %s", adminProxy, wantAdmin)
			}
			choose(wantNext)
		})
	}
}

func TestManualProxyUnrelatedSavePreservesRotation(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SaveGlobalConfig(manualProxyConfig()); err != nil {
		t.Fatal(err)
	}
	old, _ := st.GlobalConfig()
	g := New(st, nil)
	choose := func(cfg store.GlobalConfig, want string) {
		t.Helper()
		selected, err := g.selectedForKey(store.Key{ID: "key"}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if selected.proxyURL != want {
			t.Fatalf("proxy = %s, want %s", selected.proxyURL, want)
		}
	}
	choose(old, manualProxyA)
	current := old
	current.GlobalMaxConcurrency = 10
	if err := st.SaveGlobalConfig(current); err != nil {
		t.Fatal(err)
	}
	current, _ = st.GlobalConfig()
	if current.RuntimeRevision() <= old.RuntimeRevision() {
		t.Fatal("save did not publish a newer config revision")
	}
	choose(current, manualProxyB)
	choose(old, manualProxyC)
	choose(current, manualProxyA)
}

func TestManualProxyConcurrentStaleSnapshotsCannotRollback(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := manualProxyConfig()
	cfg.DefaultProxyURL = "http://removed.example:8080"
	if err := st.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	old, _ := st.GlobalConfig()
	if err := st.SaveGlobalConfig(manualProxyConfig()); err != nil {
		t.Fatal(err)
	}
	current, _ := st.GlobalConfig()
	g := New(st, nil)
	if _, err := g.selectedForKey(store.Key{ID: "key"}, current); err != nil {
		t.Fatal(err)
	}
	const requests = 600
	results := make(chan string, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		cfg := current
		if i%2 == 0 {
			cfg = old
		}
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
	for _, proxy := range strings.Split(current.DefaultProxyURL, "\n") {
		if counts[proxy] != requests/3 {
			t.Fatalf("stale snapshots changed active rotation: counts = %v", counts)
		}
	}
}
