package admin

import (
	"api-fucker/internal/store"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func Test429ConfigAndAdminStatus(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer token")
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res
	}
	res := call("PUT", "/api/admin/config", `{"upstreamBaseUrl":"https://example.invalid","key429Threshold":2,"key429CooldownMinutes":3}`)
	if res.Code != 200 {
		t.Fatalf("save config: %s", res.Body.String())
	}
	cfg, err := h.store.GlobalConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Key429Threshold != 2 || cfg.Key429CooldownMinutes != 3 {
		t.Fatalf("config not saved: %+v", cfg)
	}
	for _, body := range []string{`{"upstreamBaseUrl":"https://example.invalid","key429Threshold":-1}`, `{"upstreamBaseUrl":"https://example.invalid","key429CooldownMinutes":-1}`, `{"upstreamBaseUrl":"https://example.invalid","key429Threshold":1.5}`} {
		if res := call("PUT", "/api/admin/config", body); res.Code != 400 {
			t.Fatalf("bad config accepted %s", body)
		}
	}
	key, err := h.store.CreateKey(store.Key{Name: "cooldown", APIKey: "test-placeholder"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := h.store.RecordRateLimit(key.ID, "429", 1); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.store.CreateKey(store.Key{Name: "expired", APIKey: "test-expired", Consecutive429: 2, RateLimitedUntil: time.Now().Add(-time.Minute)}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		filter string
		total  int
	}{{"rateLimited", 1}, {"unavailable", 1}, {"routable", 1}, {"all", 2}} {
		res := call("GET", "/api/admin/keys?status="+tc.filter, "")
		var data keyListResponse
		if err := json.Unmarshal(res.Body.Bytes(), &data); err != nil {
			t.Fatal(err)
		}
		if data.Total != tc.total || data.Stats.Paused != 1 || data.Stats.Healthy != 1 {
			t.Fatalf("filter %s: %+v", tc.filter, data)
		}
		if tc.filter == "rateLimited" && (!data.Items[0].RateLimited || data.Items[0].LastStatusCode != 429 || data.Items[0].RateLimitedUntil.IsZero()) {
			t.Fatal("missing cooldown view")
		}
	}
	if res := call("POST", "/api/admin/keys/"+key.ID+"/restore", ""); res.Code != 200 {
		t.Fatal("restore failed")
	}
	res = call("GET", "/api/admin/keys?status=rateLimited", "")
	var data keyListResponse
	if err := json.Unmarshal(res.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if data.Total != 0 {
		t.Fatal("restored key still limited")
	}
}
