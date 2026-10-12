package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"api-fucker/internal/store"
)

func TestConfigManualProxyList(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()
	put := func(cfg store.GlobalConfig) *httptest.ResponseRecorder {
		t.Helper()
		body, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPut, "/api/admin/config", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer token")
		res := httptest.NewRecorder()
		h.ServeHTTP(res, req)
		return res
	}
	get := func() store.GlobalConfig {
		t.Helper()
		res := adminGet(h, "/api/admin/config")
		if res.Code != http.StatusOK {
			t.Fatalf("get config status = %d", res.Code)
		}
		var cfg store.GlobalConfig
		if err := json.Unmarshal(res.Body.Bytes(), &cfg); err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	for _, tc := range []struct {
		name, input, want string
	}{
		{"legacy", "http://legacy.example:8080", "http://legacy.example:8080"},
		{"multiple", "\n http://a.example:8080 \r\n\nhttps://b.example:8443\r\nhttp://a.example:8080\n", "http://a.example:8080\nhttps://b.example:8443"},
		{"clear", " \r\n\n", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := store.GlobalConfig{UpstreamBaseURL: "https://upstream.example", DefaultProxyURL: tc.input, ProxyMode: store.ProxyModeKeyBinding}
			if res := put(cfg); res.Code != http.StatusOK {
				t.Fatalf("save config status = %d", res.Code)
			}
			saved := get()
			if saved.DefaultProxyURL != tc.want || saved.ProxyMode != store.ProxyModeKeyBinding {
				t.Fatal("config API did not preserve normalized proxy list and mode")
			}
			invalid := saved
			invalid.DefaultProxyURL = "\nhttp://a.example:8080\nhttp://test-user:test-password@invalid.example/%zz"
			invalid.ProxyPoolEnabled = true
			res := put(invalid)
			if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "line 3") {
				t.Fatalf("invalid config status = %d, expected line 3 validation error", res.Code)
			}
			if strings.Contains(res.Body.String(), "test-user") || strings.Contains(res.Body.String(), "test-password") {
				t.Fatal("validation response exposed proxy credentials")
			}
			if !reflect.DeepEqual(get(), saved) {
				t.Fatal("rejected save changed configuration")
			}
		})
	}
}
