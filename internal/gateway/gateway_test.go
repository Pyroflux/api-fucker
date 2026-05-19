package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	"api-fucker/internal/store"
)

func TestGatewayForwardsAndRecordsCapture(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", BaseURL: "https://upstream.example", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}

	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Authorization") != "Bearer secret" {
				t.Fatalf("missing upstream auth: %s", r.Header.Get("Authorization"))
			}
			if trace := httptrace.ContextClientTrace(r.Context()); trace != nil && trace.GotFirstResponseByte != nil {
				trace.GotFirstResponseByte()
			}
			var body bytes.Buffer
			_ = json.NewEncoder(&body).Encode(map[string]string{"ok": "true"})
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(&body),
				Request:    r,
			}, nil
		})}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body %s", rec.Code, rec.Body.String())
	}
	capture, err := st.GetCapture(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if capture.StatusCode != http.StatusOK || !strings.Contains(capture.ResponseBody, "ok") {
		t.Fatalf("bad capture: %+v", capture)
	}
	text := captureNetworkText(capture.NetworkEvents)
	if !strings.Contains(text, "准备直连上游") || !strings.Contains(text, "响应读取完成，状态码 200") {
		t.Fatalf("missing direct network events: %s", text)
	}
	if strings.Contains(text, "代理") {
		t.Fatalf("direct capture should not mention proxy: %s", text)
	}
	updated, err := st.GetKey(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.LastFirstByteMS == nil {
		t.Fatalf("expected last first byte to be recorded: %+v", updated)
	}
	points, err := st.ListMetrics("minute", time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 || points[0].Requests != 1 || points[0].Errors != 0 || points[0].Concurrency != 1 || points[0].FirstByteMS == nil {
		t.Fatalf("unexpected metrics: %+v", points)
	}
}

func TestGatewayUsesGlobalBaseURLWhenKeyHasNoOverride(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if err := st.SaveGlobalConfig(store.GlobalConfig{UpstreamBaseURL: "https://global.example"}); err != nil {
		t.Fatal(err)
	}
	_, err = st.CreateKey(store.Key{Name: "k1", APIKey: "secret", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}

	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.String() != "https://global.example/v1/chat/completions" {
				t.Fatalf("unexpected upstream url: %s", r.URL.String())
			}
			var body bytes.Buffer
			_ = json.NewEncoder(&body).Encode(map[string]string{"ok": "true"})
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(&body),
				Request:    r,
			}, nil
		})}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayForwardsModelsList(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", BaseURL: "https://upstream.example", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}

	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodGet {
				t.Fatalf("method = %s, want GET", r.Method)
			}
			if r.URL.String() != "https://upstream.example/v1/models" {
				t.Fatalf("unexpected upstream url: %s", r.URL.String())
			}
			if r.Header.Get("Authorization") != "Bearer secret" {
				t.Fatalf("missing upstream auth: %s", r.Header.Get("Authorization"))
			}
			return jsonResponse(r, http.StatusOK, map[string]any{
				"object": "list",
				"data":   []map[string]string{{"id": "deepseek-ai/deepseek-v4-flash"}},
			}), nil
		})}, nil
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "deepseek-ai/deepseek-v4-flash") {
		t.Fatalf("unexpected body: %s", rec.Body.String())
	}
	if _, err := st.GetCapture(key.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("models response should not be captured, got %v", err)
	}
	points, err := st.ListMetrics("minute", time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 0 {
		t.Fatalf("models response should not be recorded in metrics: %+v", points)
	}
	updated, err := st.GetKey(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.LastUsedAt.IsZero() || updated.ConsecutiveErrors != 0 || updated.LastFirstByteMS != nil {
		t.Fatalf("models response should not update key usage state: %+v", updated)
	}
}

func TestGatewayListsModelsForSpecificKey(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key := store.Key{ID: "k1", Name: "k1", APIKey: "secret", BaseURL: "https://upstream.example", MaxConcurrency: 1, Enabled: false}
	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodGet || r.URL.String() != "https://upstream.example/v1/models" {
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
			}
			return jsonResponse(r, http.StatusOK, map[string]any{
				"data": []map[string]string{{"id": "model-a"}, {"id": "model-b"}},
			}), nil
		})}, nil
	}

	result, err := gw.ListModelsForKey(context.Background(), key, store.GlobalConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(result.Models, ",") != "model-a,model-b" {
		t.Fatalf("models = %#v", result.Models)
	}
}

func TestGatewayTestKeyRecordsSuccess(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", BaseURL: "https://upstream.example", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodPost || r.URL.String() != "https://upstream.example/v1/chat/completions" {
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
			}
			return jsonResponse(r, http.StatusOK, map[string]string{"ok": "true"}), nil
		})}, nil
	}

	result, err := gw.TestKey(context.Background(), key, store.GlobalConfig{}, "model-a", "ping")
	if err != nil {
		t.Fatal(err)
	}
	if result.StatusCode != http.StatusOK || !strings.Contains(result.ResponseBody, "ok") {
		t.Fatalf("unexpected result: %+v", result)
	}
	updated, err := st.GetKey(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.LastUsedAt.IsZero() || updated.ConsecutiveErrors != 0 || updated.LastError != "" {
		t.Fatalf("unexpected updated key: %+v", updated)
	}
	capture, err := st.GetCapture(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	if capture.RequestURL != "https://upstream.example/v1/chat/completions" || !strings.Contains(capture.RequestBody, "model-a") {
		t.Fatalf("unexpected capture: %+v", capture)
	}
}

func TestGatewayTestKeyProgressShowsSanitizedProxy(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key, err := st.CreateKey(store.Key{
		Name:           "k1",
		APIKey:         "secret",
		BaseURL:        "https://upstream.example",
		ProxyURL:       "http://user:password@proxy.example:823",
		MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gw := New(st, nil)
	gw.newClient = func(proxyURL string) (*http.Client, error) {
		if proxyURL != key.ProxyURL {
			t.Fatalf("proxyURL = %q, want %q", proxyURL, key.ProxyURL)
		}
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return jsonResponse(r, http.StatusOK, map[string]string{"ok": "true"}), nil
		})}, nil
	}

	var events []TestProgressEvent
	_, err = gw.TestKeyWithProgress(context.Background(), key, store.GlobalConfig{}, "model-a", "ping", func(event TestProgressEvent) {
		events = append(events, event)
	})
	if err != nil {
		t.Fatal(err)
	}

	text := progressText(events)
	if !strings.Contains(text, "使用代理：http://proxy.example:823") {
		t.Fatalf("missing sanitized proxy event: %s", text)
	}
	if strings.Contains(text, "user") || strings.Contains(text, "password") {
		t.Fatalf("proxy credentials leaked in progress: %s", text)
	}
	if !strings.Contains(text, "准备通过代理连接上游") {
		t.Fatalf("missing proxy request ready text: %s", text)
	}
}

func TestGatewayCaptureNetworkEventsSanitizeProxy(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key, err := st.CreateKey(store.Key{
		Name:           "k1",
		APIKey:         "secret",
		BaseURL:        "https://upstream.example",
		ProxyURL:       "http://user:password@proxy.example:823",
		MaxConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	gw := New(st, nil)
	gw.newClient = func(proxyURL string) (*http.Client, error) {
		if proxyURL != key.ProxyURL {
			t.Fatalf("proxyURL = %q, want %q", proxyURL, key.ProxyURL)
		}
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return jsonResponse(r, http.StatusOK, map[string]string{"ok": "true"}), nil
		})}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body %s", rec.Code, rec.Body.String())
	}
	capture, err := st.GetCapture(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	text := captureNetworkText(capture.NetworkEvents)
	if !strings.Contains(text, "使用代理：http://proxy.example:823") || !strings.Contains(text, "准备通过代理连接上游") {
		t.Fatalf("missing proxy capture events: %s", text)
	}
	if strings.Contains(text, "user") || strings.Contains(text, "password") {
		t.Fatalf("proxy credentials leaked in capture events: %s", text)
	}
}

func TestGatewayTestKeySavesNetworkEvents(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", BaseURL: "https://upstream.example", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return jsonResponse(r, http.StatusOK, map[string]string{"ok": "true"}), nil
		})}, nil
	}

	if _, err := gw.TestKey(context.Background(), key, store.GlobalConfig{}, "model-a", "ping"); err != nil {
		t.Fatal(err)
	}
	capture, err := st.GetCapture(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	text := captureNetworkText(capture.NetworkEvents)
	if !strings.Contains(text, "准备直连上游") || !strings.Contains(text, "响应读取完成，状态码 200") {
		t.Fatalf("missing test network events: %s", text)
	}
	if strings.Contains(text, "写入测试结果") || strings.Contains(text, "测试请求完成") {
		t.Fatalf("non-network events should not be captured: %s", text)
	}
}

func TestGatewayStreamSavesNetworkEvents(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	key, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", BaseURL: "https://upstream.example", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Header.Get("Accept") != "text/event-stream" {
				t.Fatalf("Accept = %q, want text/event-stream", r.Header.Get("Accept"))
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader("data: ok\n\n")),
				Request:    r,
			}, nil
		})}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","stream":true,"messages":[]}`))
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body %s", rec.Code, rec.Body.String())
	}
	capture, err := st.GetCapture(key.ID)
	if err != nil {
		t.Fatal(err)
	}
	text := captureNetworkText(capture.NetworkEvents)
	if !strings.Contains(text, "开始第 1 次请求上游") || !strings.Contains(text, "流式响应读取完成，状态码 200") {
		t.Fatalf("missing stream network events: %s", text)
	}
}

func TestTraceProgressUsesDirectWording(t *testing.T) {
	events := collectTraceProgress("")
	text := progressText(events)

	if !strings.Contains(text, "开始解析上游域名：integrate.api.nvidia.com") {
		t.Fatalf("missing direct DNS text: %s", text)
	}
	if !strings.Contains(text, "开始连接上游服务器：integrate.api.nvidia.com:443") {
		t.Fatalf("missing direct connect text: %s", text)
	}
	if !strings.Contains(text, "已建立新的上游连接") {
		t.Fatalf("missing direct connection text: %s", text)
	}
	if strings.Contains(text, "代理") {
		t.Fatalf("direct trace should not mention proxy: %s", text)
	}
}

func TestTraceProgressUsesProxyWording(t *testing.T) {
	events := collectTraceProgress("http://user:password@proxy.example:823")
	text := progressText(events)

	if !strings.Contains(text, "开始解析代理域名：proxy.example") {
		t.Fatalf("missing proxy DNS text: %s", text)
	}
	if !strings.Contains(text, "开始连接代理服务器：proxy.example:823") {
		t.Fatalf("missing proxy connect text: %s", text)
	}
	if !strings.Contains(text, "已建立新的代理连接") {
		t.Fatalf("missing proxy connection text: %s", text)
	}
	if !strings.Contains(text, "代理隧道已建立，准备与上游 TLS 握手") {
		t.Fatalf("missing proxy tunnel text: %s", text)
	}
	if !strings.Contains(text, "开始上游 TLS 握手") {
		t.Fatalf("missing upstream TLS text: %s", text)
	}
}

func TestCaptureProgressRecorderAddsElapsedMS(t *testing.T) {
	recorder := newCaptureProgressRecorder(nil)
	recorder.emit(TestProgressEvent{Step: "request_ready", Message: "ready"})
	time.Sleep(2 * time.Millisecond)
	recorder.emit(TestProgressEvent{Step: "response_read", Message: "done"})
	events := recorder.events()
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[0].ElapsedMS < 0 || events[1].ElapsedMS < events[0].ElapsedMS {
		t.Fatalf("elapsed ms should be non-negative and ordered: %+v", events)
	}
}

func TestGatewayReturns503WhenUpstreamNotConfigured(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	_, err = st.CreateKey(store.Key{Name: "k1", APIKey: "secret", MaxConcurrency: 1})
	if err != nil {
		t.Fatal(err)
	}

	gw := New(st, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 got %d", rec.Code)
	}
}

func TestGatewayProxyPoolRoundRobinUsesOnlineNodes(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SaveGlobalConfig(store.GlobalConfig{
		UpstreamBaseURL:  "https://upstream.example",
		DefaultProxyURL:  "http://global.example:8080",
		ProxyPoolEnabled: true,
		ProxyMode:        store.ProxyModeRoundRobin,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", MaxConcurrency: 10}); err != nil {
		t.Fatal(err)
	}
	for _, node := range []store.ProxyNodeReport{
		{ID: "node-a", Name: "a", ProxyURL: "http://10.0.0.1:9070", MaxConcurrency: 10},
		{ID: "node-b", Name: "b", ProxyURL: "http://10.0.0.2:9070", MaxConcurrency: 10},
	} {
		if _, err := st.UpsertProxyNodeReport(node, "127.0.0.1:1"); err != nil {
			t.Fatal(err)
		}
	}

	var proxies []string
	gw := New(st, nil, "node-token")
	gw.newClient = func(proxyURL string) (*http.Client, error) {
		proxies = append(proxies, proxyURL)
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return jsonResponse(r, http.StatusOK, map[string]bool{"ok": true}), nil
		})}, nil
	}
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
		rec := httptest.NewRecorder()
		gw.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d status = %d body = %s", i, rec.Code, rec.Body.String())
		}
	}
	if len(proxies) != 2 {
		t.Fatalf("proxies = %+v", proxies)
	}
	if !strings.Contains(proxies[0], "10.0.0.1:9070") || !strings.Contains(proxies[1], "10.0.0.2:9070") {
		t.Fatalf("expected round robin proxies, got %+v", proxies)
	}
	if !strings.Contains(proxies[0], "node-token") {
		t.Fatalf("expected proxy auth token injection, got %q", proxies[0])
	}
}

func TestGatewayProxyPoolReturns503WithoutAvailableNode(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.SaveGlobalConfig(store.GlobalConfig{UpstreamBaseURL: "https://upstream.example", ProxyPoolEnabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", MaxConcurrency: 1}); err != nil {
		t.Fatal(err)
	}
	gw := New(st, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}

func TestGatewayReturns429WhenNoKeyAvailable(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	gw := New(st, nil)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 got %d", rec.Code)
	}
}

func TestGatewayRetriesTransientTransportError(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", BaseURL: "https://upstream.example", MaxConcurrency: 1}); err != nil {
		t.Fatal(err)
	}

	calls := 0
	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return nil, io.ErrUnexpectedEOF
			}
			return jsonResponse(r, http.StatusOK, map[string]string{"ok": "true"}), nil
		})}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body %s", rec.Code, rec.Body.String())
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestGatewayRetriesGatewayTimeoutStatus(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", BaseURL: "https://upstream.example", MaxConcurrency: 1}); err != nil {
		t.Fatal(err)
	}

	calls := 0
	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return jsonResponse(r, http.StatusGatewayTimeout, nil), nil
			}
			return jsonResponse(r, http.StatusOK, map[string]string{"ok": "true"}), nil
		})}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d body %s", rec.Code, rec.Body.String())
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestGatewayDoesNotRetryCanceledRequest(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.CreateKey(store.Key{Name: "k1", APIKey: "secret", BaseURL: "https://upstream.example", MaxConcurrency: 1}); err != nil {
		t.Fatal(err)
	}

	calls := 0
	gw := New(st, nil)
	gw.newClient = func(string) (*http.Client, error) {
		return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			return nil, context.Canceled
		})}, nil
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"x","messages":[]}`))
	ctx, cancel := context.WithCancel(req.Context())
	cancel()
	rec := httptest.NewRecorder()
	gw.ServeHTTP(rec, req.WithContext(ctx))

	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestHTTPClientForProxyDisablesHTTP2(t *testing.T) {
	client, err := httpClientForProxy("")
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", client.Transport)
	}
	if transport.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 = true, want false")
	}
	if transport.TLSNextProto == nil {
		t.Fatal("TLSNextProto = nil, want empty map to disable HTTP/2")
	}
	if transport.TLSClientConfig == nil {
		t.Fatal("TLSClientConfig = nil")
	}
	if got := strings.Join(transport.TLSClientConfig.NextProtos, ","); got != "http/1.1" {
		t.Fatalf("NextProtos = %q, want http/1.1", got)
	}
	if transport.TLSHandshakeTimeout != upstreamTLSHandshakeLimit {
		t.Fatalf("TLSHandshakeTimeout = %s, want %s", transport.TLSHandshakeTimeout, upstreamTLSHandshakeLimit)
	}
}

func TestGatewayCachesClientPerProxy(t *testing.T) {
	gw := New(nil, nil)
	created := 0
	gw.newClient = func(string) (*http.Client, error) {
		created++
		return &http.Client{}, nil
	}

	first, err := gw.clientForProxy(" http://proxy.example:8080 ")
	if err != nil {
		t.Fatal(err)
	}
	second, err := gw.clientForProxy("http://proxy.example:8080")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("expected cached client")
	}
	if created != 1 {
		t.Fatalf("created = %d, want 1", created)
	}
}

func collectTraceProgress(proxyURL string) []TestProgressEvent {
	var events []TestProgressEvent
	trace := traceProgress(func(event TestProgressEvent) {
		events = append(events, event)
	}, 1, proxyURL)
	host := "integrate.api.nvidia.com"
	addr := "integrate.api.nvidia.com:443"
	if strings.TrimSpace(proxyURL) != "" {
		host = "proxy.example"
		addr = "proxy.example:823"
	}
	trace.DNSStart(httptrace.DNSStartInfo{Host: host})
	trace.DNSDone(httptrace.DNSDoneInfo{})
	trace.ConnectStart("tcp", addr)
	trace.ConnectDone("tcp", addr, nil)
	trace.GotConn(httptrace.GotConnInfo{Reused: false})
	trace.TLSHandshakeStart()
	trace.TLSHandshakeDone(tls.ConnectionState{NegotiatedProtocol: "http/1.1"}, nil)
	return events
}

func progressText(events []TestProgressEvent) string {
	var messages []string
	for _, event := range events {
		messages = append(messages, event.Message)
		if event.Error != "" {
			messages = append(messages, event.Error)
		}
	}
	return strings.Join(messages, "\n")
}

func captureNetworkText(events []store.CaptureNetworkEvent) string {
	var messages []string
	for _, event := range events {
		messages = append(messages, event.Message)
		if event.Error != "" {
			messages = append(messages, event.Error)
		}
	}
	return strings.Join(messages, "\n")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return fn(r)
}

func jsonResponse(req *http.Request, status int, value any) *http.Response {
	var body bytes.Buffer
	if value != nil {
		_ = json.NewEncoder(&body).Encode(value)
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(&body),
		Request:    req,
	}
}
