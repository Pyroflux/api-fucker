package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"api-fucker/internal/store"
)

func rotationGateway(t *testing.T, ids ...string) *Gateway {
	t.Helper()
	st, err := store.Open(t.TempDir() + "/rotation.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.SaveGlobalConfig(store.GlobalConfig{UpstreamBaseURL: "https://example.invalid"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, err := st.CreateKey(store.Key{ID: id, Name: id, APIKey: "test-placeholder", MaxConcurrency: 1}); err != nil {
			t.Fatal(err)
		}
	}
	return New(st, nil)
}

func expectRotationKey(t *testing.T, g *Gateway, id string) selectedKey {
	t.Helper()
	k, err := g.acquireKey()
	if err != nil {
		t.Fatal(err)
	}
	if k.key.ID != id {
		t.Fatalf("selected %s, want %s", k.key.ID, id)
	}
	return k
}

func TestRotationAlternatingCompletions(t *testing.T) {
	g := rotationGateway(t, "a", "b", "c")
	var held []selectedKey
	for i := 0; i < 12; i++ {
		held = append(held, expectRotationKey(t, g, []string{"a", "b", "c"}[i%3]))
		if len(held) == 2 {
			g.releaseSelected(held[0])
			held = held[1:]
		}
	}
	g.releaseSelected(held[0])
}

func TestRotationSkipsAndRejoins(t *testing.T) {
	for _, state := range []string{"disabled", "manual", "paused", "balance", "cooldown", "busy", "rpm"} {
		t.Run(state, func(t *testing.T) {
			g := rotationGateway(t, "a", "b", "c")
			g.releaseSelected(expectRotationKey(t, g, "a"))
			k, _ := g.store.GetKey("b")
			switch state {
			case "disabled":
				k.Enabled = false
			case "paused":
				k.Paused = true
			case "balance":
				k.BalanceDepleted = true
			case "manual":
				if _, err := g.store.PauseKeys([]string{"b"}); err != nil {
					t.Fatal(err)
				}
			case "cooldown":
				if _, err := g.store.RecordRateLimit("b", "test", 0); err != nil {
					t.Fatal(err)
				}
			case "busy":
				g.inflight["b"] = 1
			case "rpm":
				cfg, _ := g.store.GlobalConfig()
				cfg.KeyMaxRequestsPerMinute = 1
				if err := g.store.SaveGlobalConfig(cfg); err != nil {
					t.Fatal(err)
				}
				g.keyMinute["b"] = &minuteCounter{hits: []time.Time{time.Now()}}
			}
			if state == "disabled" || state == "paused" || state == "balance" {
				if _, err := g.store.UpdateKey("b", k); err != nil {
					t.Fatal(err)
				}
			}
			g.releaseSelected(expectRotationKey(t, g, "c"))
			k.Enabled = true
			k.Paused = false
			k.BalanceDepleted = false
			if _, err := g.store.UpdateKey("b", k); err != nil {
				t.Fatal(err)
			}
			if _, err := g.store.RestoreKey("b"); err != nil {
				t.Fatal(err)
			}
			g.inflight["b"] = 0
			cfg, _ := g.store.GlobalConfig()
			cfg.KeyMaxRequestsPerMinute = 0
			if err := g.store.SaveGlobalConfig(cfg); err != nil {
				t.Fatal(err)
			}
			g.releaseSelected(expectRotationKey(t, g, "a"))
			g.releaseSelected(expectRotationKey(t, g, "b"))
		})
	}
}

func TestRotationMembershipChanges(t *testing.T) {
	g := rotationGateway(t, "b", "d", "f")
	g.releaseSelected(expectRotationKey(t, g, "b"))
	if err := g.store.DeleteKey("b"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "c"} {
		if _, err := g.store.CreateKey(store.Key{ID: id, Name: id, APIKey: "test-placeholder"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"c", "d", "f", "a"} {
		g.releaseSelected(expectRotationKey(t, g, id))
	}
}

func TestRotationConcurrentReservations(t *testing.T) {
	g := rotationGateway(t, "a", "b", "c")
	var wg sync.WaitGroup
	selected := make(chan selectedKey, 30)
	start := make(chan struct{})
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			k, err := g.acquireKey()
			if err == nil {
				selected <- k
			} else if !errors.Is(err, errNoAvailableKey) {
				t.Errorf("acquire: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(selected)
	seen := map[string]bool{}
	for k := range selected {
		if seen[k.key.ID] {
			t.Errorf("duplicate concurrent reservation: %s", k.key.ID)
		}
		seen[k.key.ID] = true
		g.releaseSelected(k)
	}
	if len(seen) != 3 {
		t.Fatalf("reservations=%v", seen)
	}
	for _, n := range g.CurrentConcurrency() {
		if n != 0 {
			t.Fatal("reservation leak")
		}
	}
}

func TestRotationFailedAdmissionDoesNotCount(t *testing.T) {
	g := rotationGateway(t, "a")
	cfg, _ := g.store.GlobalConfig()
	cfg.ProxyPoolEnabled = true
	if err := g.store.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := g.acquireKey(); !errors.Is(err, errNoAvailableProxy) {
		t.Fatalf("error=%v", err)
	}
	if g.KeyMinuteUsageForKey("a") != 0 || g.CurrentConcurrency()["a"] != 0 {
		t.Fatal("failed admission consumed budget")
	}
}

func TestMinuteCounterBoundaryAndMemory(t *testing.T) {
	now := time.Now()
	mc := &minuteCounter{hits: []time.Time{now.Add(-time.Minute), now.Add(-time.Minute + time.Nanosecond), now}}
	mc.prune(now)
	if mc.count() != 2 {
		t.Fatalf("boundary count=%d", mc.count())
	}
	mc.prune(now.Add(time.Minute))
	if mc.count() != 0 || mc.hits != nil || mc.head != 0 {
		t.Fatal("expired queue retained memory")
	}
	hits := make([]time.Time, 1000)
	for i := range hits {
		hits[i] = now.Add(-time.Second)
	}
	hits[0] = now.Add(-time.Minute)
	mc = &minuteCounter{hits: hits}
	mc.prune(now)
	if mc.head != 1 || &mc.hits[0] != &hits[0] {
		t.Fatal("small expired prefix unnecessarily copied")
	}
	mc.prune(now.Add(time.Minute))
	if mc.hits != nil {
		t.Fatal("peak allocation retained")
	}
}

func TestMinuteUsageSweepAndIsolation(t *testing.T) {
	g := rotationGateway(t, "a", "b")
	now := time.Now()
	g.keyMinute["a"] = &minuteCounter{hits: []time.Time{now}}
	g.keyMinute["b"] = &minuteCounter{hits: []time.Time{now.Add(-2 * time.Minute)}}
	g.keyMinute["deleted"] = &minuteCounter{hits: []time.Time{now}}
	if g.KeyMinuteUsageForKey("a") != 1 || len(g.keyMinute) != 3 {
		t.Fatal("single-key lookup touched other counters")
	}
	keys, _ := g.store.ListKeys()
	g.pruneKeyMinuteLocked(keys, now)
	if len(g.keyMinute) != 1 {
		t.Fatal("sweep did not drop expired/deleted keys")
	}
	g.keyMinute["deleted"] = &minuteCounter{hits: []time.Time{now}}
	g.pruneKeyMinuteLocked(keys, now.Add(time.Second))
	if g.keyMinute["deleted"] == nil {
		t.Fatal("sweep ran too frequently")
	}
	g.pruneKeyMinuteLocked(keys, now.Add(time.Minute))
	if len(g.keyMinute) != 0 {
		t.Fatal("idle counts retained after expiry")
	}
}

func TestMinuteUsageLogicalRequestsAndRestart(t *testing.T) {
	g := rotationGateway(t, "a") // RPM disabled: counts must still be recorded.
	calls := 0
	g.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			if calls < 3 {
				return nil, fmt.Errorf("simulated network failure")
			}
			return jsonResponse(r, 500, map[string]string{"error": "test"}), nil
		})}, nil
	}
	g.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", chatPath, strings.NewReader(`{}`)))
	if calls != 3 || g.KeyMinuteUsageForKey("a") != 1 {
		t.Fatalf("calls=%d usage=%d", calls, g.KeyMinuteUsageForKey("a"))
	}
	k, _ := g.store.GetKey("a")
	cfg, _ := g.store.GlobalConfig()
	if _, err := g.TestKey(context.Background(), k, cfg, "test", "ping"); err != nil {
		t.Fatal(err)
	}
	if g.KeyMinuteUsageForKey("a") != 1 {
		t.Fatal("manual test affected routing count")
	}
	fresh := New(g.store, nil)
	if fresh.KeyMinuteUsageForKey("a") != 0 {
		t.Fatal("restart imported historical requests")
	}
	g.keyMinute["a"] = &minuteCounter{hits: []time.Time{time.Now().Add(-2 * time.Minute)}}
	if g.KeyMinuteUsageForKey("a") != 0 || len(g.keyMinute) != 0 {
		t.Fatal("expired single-key counter not released")
	}
}

func BenchmarkSingleKeyMinuteUsage(b *testing.B) {
	for _, size := range []int{100, 7300} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			g := New(nil, nil)
			now := time.Now()
			for i := 0; i < size; i++ {
				g.keyMinute[fmt.Sprint(i)] = &minuteCounter{hits: []time.Time{now}}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				g.KeyMinuteUsageForKey("0")
			}
		})
	}
}
