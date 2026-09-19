package store

import (
	"sync"
	"testing"
	"time"
)

func Test429CooldownStateMachine(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	key, err := st.CreateKey(Key{Name: "cooldown", APIKey: "test-placeholder"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := defaultGlobalConfig()
	cfg.Key429Threshold = 2
	cfg.Key429CooldownMinutes = 7
	now := time.Now().UTC()
	record := func(at time.Time) Key {
		t.Helper()
		k, e := st.recordRateLimitAt(key.ID, "quota balance exceeded", 12, cfg, at)
		if e != nil {
			t.Fatal(e)
		}
		return k
	}
	k := record(now)
	if k.Consecutive429 != 1 || k.IsRateLimited(now) || k.Paused || k.BalanceDepleted || k.ConsecutiveErrors != 0 {
		t.Fatalf("first 429: %+v", k)
	}
	if err := st.RecordSuccess(key.ID); err != nil {
		t.Fatal(err)
	}
	k = record(now)
	if k.Consecutive429 != 1 {
		t.Fatal("success did not reset sequence")
	}
	if _, err := st.RecordFailure(key.ID, "upstream failed", false); err != nil {
		t.Fatal(err)
	}
	k = record(now)
	if k.Consecutive429 != 1 || k.ConsecutiveErrors != 0 {
		t.Fatal("non-429 failure or 429 did not break corresponding sequence")
	}
	k = record(now)
	until := now.Add(7 * time.Minute)
	if !k.RateLimitedUntil.Equal(until) || k.Paused || k.BalanceDepleted {
		t.Fatalf("bad cooldown: %+v", k)
	}
	cfg.Key429CooldownMinutes = 20
	k = record(now.Add(time.Minute))
	if !k.RateLimitedUntil.Equal(until) {
		t.Fatal("extra 429 extended cooldown")
	}
	if err := st.RecordSuccess(key.ID); err != nil {
		t.Fatal(err)
	}
	k, err = st.GetKey(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !k.RateLimitedUntil.Equal(until) || k.Consecutive429 != 0 {
		t.Fatal("late/manual-test success must reset sequence but preserve cooldown")
	}
	if k.IsRateLimited(until) || !k.IsRateLimited(until.Add(-time.Nanosecond)) {
		t.Fatal("wrong expiry boundary")
	}
	k = record(until)
	if k.Consecutive429 != 1 || k.IsRateLimited(until) {
		t.Fatal("new cooldown cycle did not start fresh")
	}
	k = record(until)
	if !k.RateLimitedUntil.Equal(until.Add(20 * time.Minute)) {
		t.Fatal("new config not used for next cooldown")
	}
	if _, err := st.PauseKeys([]string{key.ID}); err != nil {
		t.Fatal(err)
	}
	k = record(until.Add(21 * time.Minute))
	if !k.ManualPaused {
		t.Fatal("expiry cleared manual pause")
	}
	if _, err := st.RestoreKey(key.ID); err != nil {
		t.Fatal(err)
	}
	k, _ = st.GetKey(key.ID)
	if k.Consecutive429 != 0 || !k.RateLimitedUntil.IsZero() || k.ManualPaused {
		t.Fatal("restore did not clear state")
	}
}

func Test429DefaultsPersistenceAndRestoreAll(t *testing.T) {
	path := t.TempDir() + "/data.db"
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := st.GlobalConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Key429Threshold != 1 || cfg.Key429CooldownMinutes != 10 {
		t.Fatalf("defaults %+v", cfg)
	}
	key, err := st.CreateKey(Key{Name: "cooldown", APIKey: "test-placeholder"})
	if err != nil {
		t.Fatal(err)
	}
	key, err = st.RecordRateLimit(key.ID, "429", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !key.IsRateLimited(time.Now()) {
		t.Fatal("default did not trigger")
	}
	until := key.RateLimitedUntil
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	key, err = st.GetKey(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !key.RateLimitedUntil.Equal(until) {
		t.Fatal("cooldown lost across reopen")
	}
	if n, err := st.RestoreAllKeys(); err != nil || n != 1 {
		t.Fatalf("restore all %d %v", n, err)
	}
	key, _ = st.GetKey(key.ID)
	if !key.RateLimitedUntil.IsZero() || key.Consecutive429 != 0 {
		t.Fatal("restore all retained cooldown")
	}
	for _, c := range []GlobalConfig{
		{UpstreamBaseURL: "https://example.invalid", Key429Threshold: -1},
		{UpstreamBaseURL: "https://example.invalid", Key429CooldownMinutes: -1},
		{UpstreamBaseURL: "https://example.invalid", Key429CooldownMinutes: 153722868},
	} {
		if err := st.SaveGlobalConfig(c); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	if err := st.SaveGlobalConfig(GlobalConfig{UpstreamBaseURL: "https://example.invalid"}); err != nil {
		t.Fatal(err)
	}
	cfg, _ = st.GlobalConfig()
	if cfg.Key429Threshold != 1 || cfg.Key429CooldownMinutes != 10 {
		t.Fatal("missing fields not defaulted")
	}
}

func Test429ConcurrentResultsPreserveCooldown(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	key, err := st.CreateKey(Key{Name: "cooldown", APIKey: "test-placeholder"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := defaultGlobalConfig()
	cfg.Key429Threshold = 8
	now := time.Now().UTC()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := st.recordRateLimitAt(key.ID, "429", 1, cfg, now); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	k, _ := st.GetKey(key.ID)
	if k.Consecutive429 != 8 || !k.IsRateLimited(now) {
		t.Fatalf("lost concurrent outcomes: %+v", k)
	}
	until := k.RateLimitedUntil
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i%2 == 0 {
				if err := st.RecordSuccess(key.ID); err != nil {
					t.Error(err)
				}
			} else {
				if _, err := st.RecordRateLimit(key.ID, "429", 1); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	k, _ = st.GetKey(key.ID)
	if !k.RateLimitedUntil.Equal(until) {
		t.Fatal("concurrent result changed deadline")
	}
}
