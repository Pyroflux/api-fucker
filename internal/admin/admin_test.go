package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"api-fucker/internal/gateway"
	"api-fucker/internal/store"
)

func TestLoginTrimsConfiguredToken(t *testing.T) {
	h, closeStore := newTestHandler(t, "  change-me\r\n")
	defer closeStore()

	res := postLogin(h, `{"token":"change-me"}`)

	if res.Code != http.StatusOK {
		t.Fatalf("login status = %d, want %d", res.Code, http.StatusOK)
	}
}

func TestLoginCookieAuthorizesSpecialToken(t *testing.T) {
	h, closeStore := newTestHandler(t, "secret;with,chars")
	defer closeStore()

	res := postLogin(h, `{"token":"secret;with,chars"}`)
	if res.Code != http.StatusOK {
		t.Fatalf("login status = %d, want %d", res.Code, http.StatusOK)
	}
	cookies := res.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies = %d, want 1", len(cookies))
	}
	if strings.ContainsAny(cookies[0].Value, ";,") {
		t.Fatalf("cookie value contains unsafe raw token characters: %q", cookies[0].Value)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/admin/config", nil)
	req.AddCookie(cookies[0])
	cfgRes := httptest.NewRecorder()
	h.ServeHTTP(cfgRes, req)

	if cfgRes.Code != http.StatusOK {
		t.Fatalf("authorized config status = %d, want %d", cfgRes.Code, http.StatusOK)
	}
}

func TestKeysFilterAndSort(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()

	a, err := h.store.CreateKey(store.Key{Name: "a", APIKey: "secret-a", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.store.CreateKey(store.Key{Name: "b", APIKey: "secret-b", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.UpdateKey(b.ID, store.Key{Name: b.Name, APIKey: b.APIKey, MaxConcurrency: 1, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := h.store.RecordSuccess(a.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if _, err := h.store.RecordFailure(b.ID, "boom", false); err != nil {
		t.Fatal(err)
	}

	res := adminGet(h, "/api/admin/keys?status=unavailable&sort=lastUsedAt&order=desc")
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", res.Code, res.Body.String())
	}
	var body keyListResponse
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Total != 1 || len(body.Items) != 1 || body.Items[0].ID != b.ID {
		t.Fatalf("unexpected list response: %+v", body)
	}

	res = adminGet(h, "/api/admin/keys?status=error")
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", res.Code, res.Body.String())
	}
	body = keyListResponse{}
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Total != 1 || body.Items[0].ID != b.ID {
		t.Fatalf("unexpected error filter response: %+v", body)
	}
}

func TestConfigSavesIPRules(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()

	req := httptest.NewRequest(http.MethodPut, "/api/admin/config", strings.NewReader(`{
		"upstreamBaseUrl":"https://upstream.example",
		"defaultProxyUrl":"",
		"ipAllowlist":["127.0.0.1","192.*"],
		"ipBlocklist":["10.*"]
	}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("save config status = %d body = %s", res.Code, res.Body.String())
	}

	res = adminGet(h, "/api/admin/config")
	if res.Code != http.StatusOK {
		t.Fatalf("get config status = %d body = %s", res.Code, res.Body.String())
	}
	var cfg store.GlobalConfig
	if err := json.Unmarshal(res.Body.Bytes(), &cfg); err != nil {
		t.Fatal(err)
	}
	if len(cfg.IPAllowlist) != 2 || cfg.IPAllowlist[1] != "192.*" || len(cfg.IPBlocklist) != 1 || cfg.IPBlocklist[0] != "10.*" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestBulkConcurrencyAndDeleteActions(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()

	a, err := h.store.CreateKey(store.Key{Name: "a", APIKey: "secret-a", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	b, err := h.store.CreateKey(store.Key{Name: "b", APIKey: "secret-b", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	c, err := h.store.CreateKey(store.Key{Name: "c", APIKey: "secret-c", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/admin/keys/concurrency", strings.NewReader(`{"ids":["`+a.ID+`","`+b.ID+`"],"maxConcurrency":7}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"updated":2`) {
		t.Fatalf("bulk concurrency status = %d body = %s", res.Code, res.Body.String())
	}
	updatedA, _ := h.store.GetKey(a.ID)
	updatedB, _ := h.store.GetKey(b.ID)
	updatedC, _ := h.store.GetKey(c.ID)
	if updatedA.MaxConcurrency != 7 || updatedB.MaxConcurrency != 7 || updatedC.MaxConcurrency != 1 {
		t.Fatalf("unexpected concurrency: %+v %+v %+v", updatedA, updatedB, updatedC)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/admin/keys/concurrency", strings.NewReader(`{"all":true,"maxConcurrency":3}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token")
	res = httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"updated":3`) {
		t.Fatalf("bulk concurrency all status = %d body = %s", res.Code, res.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/admin/keys/delete", strings.NewReader(`{"ids":["`+a.ID+`"]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token")
	res = httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"deleted":1`) {
		t.Fatalf("bulk delete status = %d body = %s", res.Code, res.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/admin/keys/delete", strings.NewReader(`{"all":true}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token")
	res = httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK || !strings.Contains(res.Body.String(), `"deleted":2`) {
		t.Fatalf("bulk delete all status = %d body = %s", res.Code, res.Body.String())
	}
}

func TestBulkActionsRequireSelectionUnlessAll(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()

	req := httptest.NewRequest(http.MethodPost, "/api/admin/keys/delete", strings.NewReader(`{"ids":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "no keys selected") {
		t.Fatalf("status = %d body = %s", res.Code, res.Body.String())
	}
}

func TestMetricsEndpoint(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()

	firstByte := int64(88)
	if err := h.store.RecordMetric("", store.MetricSample{Time: time.Now().UTC(), FirstByteMS: &firstByte, Concurrency: 2, Error: true}); err != nil {
		t.Fatal(err)
	}

	res := adminGet(h, "/api/admin/metrics?granularity=minute")
	if res.Code != http.StatusOK {
		t.Fatalf("metrics status = %d body = %s", res.Code, res.Body.String())
	}
	var body struct {
		Items []store.MetricPoint `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 || body.Items[0].FirstByteMS == nil || *body.Items[0].FirstByteMS != 88 || body.Items[0].Errors != 1 {
		t.Fatalf("unexpected metrics response: %+v", body)
	}
}

func TestMetricsEndpointIncludesLiveConcurrency(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "data.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	h := New(st, "token", func() map[string]int {
		return map[string]int{"a": 1, "b": 2}
	})

	res := adminGet(h, "/api/admin/metrics?granularity=minute")
	if res.Code != http.StatusOK {
		t.Fatalf("metrics status = %d body = %s", res.Code, res.Body.String())
	}
	var body struct {
		Items []store.MetricPoint `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 || body.Items[0].Concurrency != 3 || body.Items[0].Requests != 0 {
		t.Fatalf("unexpected live metrics response: %+v", body)
	}
}

func TestProxyReportAndManagementEndpoints(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()
	h.SetProxyNodeToken("node-token")

	req := httptest.NewRequest(http.MethodPost, "/api/admin/proxies/report", strings.NewReader(`{"id":"node-a","name":"vps-a","listenAddr":":9070","maxConcurrency":50}`))
	req.Header.Set("Authorization", "Bearer node-token")
	req.Header.Set("Content-Type", "application/json")
	req.RemoteAddr = "203.0.113.9:4567"
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("report status = %d body = %s", res.Code, res.Body.String())
	}

	list := adminGet(h, "/api/admin/proxies")
	if list.Code != http.StatusOK {
		t.Fatalf("list status = %d body = %s", list.Code, list.Body.String())
	}
	if !strings.Contains(list.Body.String(), "http://203.0.113.9:9070") {
		t.Fatalf("missing derived proxy url: %s", list.Body.String())
	}

	update := httptest.NewRequest(http.MethodPut, "/api/admin/proxies/node-a", strings.NewReader(`{"name":"manual","proxyUrl":"http://203.0.113.10:9070","enabled":false,"maxConcurrency":20}`))
	update.Header.Set("Authorization", "Bearer token")
	update.Header.Set("Content-Type", "application/json")
	updateRes := httptest.NewRecorder()
	h.ServeHTTP(updateRes, update)
	if updateRes.Code != http.StatusOK {
		t.Fatalf("update status = %d body = %s", updateRes.Code, updateRes.Body.String())
	}
	node, err := h.store.GetProxyNode("node-a")
	if err != nil {
		t.Fatal(err)
	}
	if node.Enabled || node.MaxConcurrency != 20 || node.Name != "manual" {
		t.Fatalf("unexpected updated proxy: %+v", node)
	}

	unauth := httptest.NewRequest(http.MethodPost, "/api/admin/proxies/report", strings.NewReader(`{"id":"node-b"}`))
	unauthRes := httptest.NewRecorder()
	h.ServeHTTP(unauthRes, unauth)
	if unauthRes.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthRes.Code)
	}
}

func TestKeyModelsAndTestActions(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()

	key, err := h.store.CreateKey(store.Key{Name: "a", APIKey: "secret-a", BaseURL: "https://upstream.example", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeUpstream{}
	h.upstream = fake

	res := adminGet(h, "/api/admin/keys/"+key.ID+"/models")
	if res.Code != http.StatusOK {
		t.Fatalf("models status = %d body = %s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "model-a") {
		t.Fatalf("unexpected models body: %s", res.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/api/admin/keys/"+key.ID+"/test", strings.NewReader(`{"model":"model-a","prompt":"ping"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token")
	res = httptest.NewRecorder()
	h.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("test status = %d body = %s", res.Code, res.Body.String())
	}
	if fake.testModel != "model-a" || fake.testPrompt != "ping" {
		t.Fatalf("unexpected fake calls: %+v", fake)
	}
}

func TestKeyTestProgressStreamsEvents(t *testing.T) {
	h, closeStore := newTestHandler(t, "token")
	defer closeStore()

	key, err := h.store.CreateKey(store.Key{Name: "a", APIKey: "secret-a", BaseURL: "https://upstream.example", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	h.upstream = &fakeUpstream{}

	req := httptest.NewRequest(http.MethodPost, "/api/admin/keys/"+key.ID+"/test-progress", strings.NewReader(`{"model":"model-a","prompt":"ping"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", res.Code, res.Body.String())
	}
	body := res.Body.String()
	if !strings.Contains(body, `"step":"fake"`) || !strings.Contains(body, `"step":"done"`) || !strings.Contains(body, `"statusCode":200`) {
		t.Fatalf("unexpected progress body: %s", body)
	}
}

func newTestHandler(t *testing.T, token string) (*Handler, func()) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "data.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return New(st, token, nil), func() {
		if err := st.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
	}
}

func postLogin(h *Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/admin/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	return res
}

func adminGet(h *Handler, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer token")
	res := httptest.NewRecorder()
	h.ServeHTTP(res, req)
	return res
}

type fakeUpstream struct {
	testModel  string
	testPrompt string
}

func (f *fakeUpstream) ListModelsForKey(context.Context, store.Key, store.GlobalConfig) (gateway.ModelListResult, error) {
	return gateway.ModelListResult{Models: []string{"model-a"}}, nil
}

func (f *fakeUpstream) TestKey(_ context.Context, _ store.Key, _ store.GlobalConfig, model string, prompt string) (gateway.UpstreamTestResult, error) {
	f.testModel = model
	f.testPrompt = prompt
	return gateway.UpstreamTestResult{StatusCode: http.StatusOK, ResponseBody: `{"ok":true}`}, nil
}

func (f *fakeUpstream) TestKeyWithProgress(ctx context.Context, key store.Key, cfg store.GlobalConfig, model string, prompt string, progress func(gateway.TestProgressEvent)) (gateway.UpstreamTestResult, error) {
	progress(gateway.TestProgressEvent{Step: "fake", Message: "fake progress"})
	return f.TestKey(ctx, key, cfg, model, prompt)
}
