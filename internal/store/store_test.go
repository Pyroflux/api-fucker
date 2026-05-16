package store

import (
	"errors"
	"testing"
)

func TestCreateRestoreAndCapture(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key, err := st.CreateKey(Key{
		Name:           "k1",
		APIKey:         "secret",
		BaseURL:        "https://upstream.example",
		MaxConcurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		key, err = st.RecordFailure(key.ID, "quota exceeded", true)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !key.Paused || !key.BalanceDepleted {
		t.Fatalf("expected paused balance-depleted key, got %+v", key)
	}

	key, err = st.RestoreKey(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if key.Paused || key.BalanceDepleted || key.ConsecutiveErrors != 0 {
		t.Fatalf("expected restored key, got %+v", key)
	}

	err = st.SaveCapture(Capture{
		KeyID:        key.ID,
		RequestBody:  `{"ok":true}`,
		ResponseBody: `{"done":true}`,
		NetworkEvents: []CaptureNetworkEvent{
			{Step: "request_ready", Message: "请求已构建，准备直连上游", ElapsedMS: 3},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	capture, err := st.GetCapture(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if capture.RequestBody == "" || capture.ResponseBody == "" {
		t.Fatalf("expected capture bodies, got %+v", capture)
	}
	if len(capture.NetworkEvents) != 1 || capture.NetworkEvents[0].Step != "request_ready" {
		t.Fatalf("expected capture network events, got %+v", capture.NetworkEvents)
	}
	if capture.NetworkEvents[0].ElapsedMS != 3 {
		t.Fatalf("expected elapsed ms, got %+v", capture.NetworkEvents[0])
	}
	if err := st.SaveGlobalConfig(GlobalConfig{
		UpstreamBaseURL: "https://upstream.example",
		IPAllowlist:     []string{"127.0.0.1\n192.*", "10.0.*"},
		IPBlocklist:     []string{"  127.0.0.2, 172.*  "},
	}); err != nil {
		t.Fatal(err)
	}
	cfg, err := st.GlobalConfig()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.IPAllowlist) != 3 || cfg.IPAllowlist[1] != "192.*" || len(cfg.IPBlocklist) != 2 || cfg.IPBlocklist[1] != "172.*" {
		t.Fatalf("unexpected normalized ip rules: %+v", cfg)
	}
}

func TestImportKeysSkipsDuplicatesAndAllowsOverrides(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	result, err := st.ImportKeys([]ImportKeyInput{
		{Name: "a", APIKey: "secret-a", BaseURL: "https://a.example", ProxyURL: "http://127.0.0.1:7890", MaxConcurrency: 2, Enabled: true},
		{Name: "dup", APIKey: "secret-a", MaxConcurrency: 1, Enabled: true},
		{Name: "b", APIKey: "secret-b", MaxConcurrency: 1, Enabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Imported != 2 || result.Skipped != 1 {
		t.Fatalf("unexpected import result: %+v", result)
	}

	keys, err := st.ListKeys()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys got %d", len(keys))
	}
}

func TestRestoreAllKeys(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	first, err := st.CreateKey(Key{Name: "a", APIKey: "secret-a", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := st.CreateKey(Key{Name: "b", APIKey: "secret-b", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.RecordFailure(first.ID, "quota exceeded", true); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RecordFailure(second.ID, "upstream error", false); err != nil {
		t.Fatal(err)
	}

	restored, err := st.RestoreAllKeys()
	if err != nil {
		t.Fatal(err)
	}
	if restored != 2 {
		t.Fatalf("expected 2 restored, got %d", restored)
	}

	keys, err := st.ListKeys()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if key.Paused || key.BalanceDepleted || key.ConsecutiveErrors != 0 || key.LastError != "" {
			t.Fatalf("expected restored key, got %+v", key)
		}
	}
}

func TestBulkUpdateConcurrencyAndDeleteKeys(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	first, err := st.CreateKey(Key{Name: "a", APIKey: "secret-a", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := st.CreateKey(Key{Name: "b", APIKey: "secret-b", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	third, err := st.CreateKey(Key{Name: "c", APIKey: "secret-c", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}

	updated, err := st.UpdateKeysMaxConcurrency([]string{first.ID, second.ID}, 5)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Updated != 2 {
		t.Fatalf("updated = %d, want 2", updated.Updated)
	}
	first, _ = st.GetKey(first.ID)
	second, _ = st.GetKey(second.ID)
	third, _ = st.GetKey(third.ID)
	if first.MaxConcurrency != 5 || second.MaxConcurrency != 5 || third.MaxConcurrency != 1 {
		t.Fatalf("unexpected concurrency: %+v %+v %+v", first, second, third)
	}

	updated, err = st.UpdateKeysMaxConcurrency(nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Updated != 3 {
		t.Fatalf("updated all = %d, want 3", updated.Updated)
	}

	if err := st.SaveCapture(Capture{KeyID: first.ID, RequestBody: "x"}); err != nil {
		t.Fatal(err)
	}
	deleted, err := st.DeleteKeys([]string{first.ID, "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted.Deleted)
	}
	if _, err := st.GetCapture(first.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected capture deletion, got %v", err)
	}

	deleted, err = st.DeleteKeys(nil)
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Deleted != 2 {
		t.Fatalf("deleted all = %d, want 2", deleted.Deleted)
	}
	keys, err := st.ListKeys()
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Fatalf("keys left = %d, want 0", len(keys))
	}
}
