package admin

import (
	"api-fucker/internal/store"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCaptureLiveMinuteUsage(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()
	key, err := h.store.CreateKey(store.Key{Name: "test", APIKey: "test-placeholder"})
	if err != nil {
		t.Fatal(err)
	}
	h.SetKeyMinuteUsage(func() map[string]int { t.Fatal("detail read full usage map"); return nil })
	usage := 0
	h.SetKeyMinuteUsageForKey(func(id string) int {
		if id != key.ID {
			t.Fatalf("wrong key %s", id)
		}
		return usage
	})
	for _, hasCapture := range []bool{false, true} {
		if hasCapture {
			usage = 7
			if err := h.store.SaveCapture(store.Capture{KeyID: key.ID, StatusCode: 200, RequestBody: "old request"}); err != nil {
				t.Fatal(err)
			}
		}
		req := httptest.NewRequest(http.MethodGet, "/api/admin/keys/"+key.ID+"/capture", nil)
		unauthorized := httptest.NewRecorder()
		h.ServeHTTP(unauthorized, req)
		if unauthorized.Code != http.StatusUnauthorized {
			t.Fatal("missing authentication")
		}
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		var got struct {
			CurrentMinuteRequests int    `json:"currentMinuteRequests"`
			HasCapture            bool   `json:"hasCapture"`
			CurrentAPIKey         string `json:"currentApiKey"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if rec.Code != 200 || rec.Header().Get("Cache-Control") != "no-store" || got.CurrentMinuteRequests != usage || got.HasCapture != hasCapture || got.CurrentAPIKey != "test-placeholder" {
			t.Fatalf("unexpected detail: %d %s", rec.Code, rec.Body.String())
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/api/admin/keys/missing/capture", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("missing key status=%d", rec.Code)
	}
}
