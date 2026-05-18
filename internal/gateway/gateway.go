package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"time"

	"api-fucker/internal/store"
)

const (
	chatPath                  = "/v1/chat/completions"
	modelsPath                = "/v1/models"
	upstreamMaxAttempts       = 3
	upstreamRetryBaseDelay    = 300 * time.Millisecond
	upstreamDialTimeout       = 30 * time.Second
	upstreamTLSHandshakeLimit = 30 * time.Second
)

var (
	errNoAvailableKey        = errors.New("no available upstream key")
	errUpstreamNotConfigured = errors.New("upstream base URL is not configured")
)

type Gateway struct {
	store *store.Store

	mu         sync.Mutex
	roundRobin int
	inflight   map[string]int
	clients    map[string]*http.Client
	newClient  func(proxyURL string) (*http.Client, error)
}

type selectedKey struct {
	key      store.Key
	baseURL  string
	proxyURL string
}

func (s selectedKey) usesProxy() bool {
	return strings.TrimSpace(s.proxyURL) != ""
}

type ModelListResult struct {
	Models []string `json:"models"`
	Raw    any      `json:"raw,omitempty"`
}

type UpstreamTestResult struct {
	StatusCode     int                 `json:"statusCode"`
	DurationMS     int64               `json:"durationMs"`
	ResponseHeader map[string][]string `json:"responseHeader"`
	ResponseBody   string              `json:"responseBody"`
	Error          string              `json:"error,omitempty"`
}

type TestProgressEvent struct {
	Step       string              `json:"step"`
	Message    string              `json:"message"`
	Attempt    int                 `json:"attempt,omitempty"`
	DurationMS int64               `json:"durationMs,omitempty"`
	ElapsedMS  int64               `json:"elapsedMs"`
	Error      string              `json:"error,omitempty"`
	Result     *UpstreamTestResult `json:"result,omitempty"`
}

func New(st *store.Store, _ *http.Client) *Gateway {
	return &Gateway{store: st, inflight: make(map[string]int), clients: make(map[string]*http.Client), newClient: httpClientForProxy}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	upstreamPath, allowedMethod, err := routeRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if r.Method != allowedMethod {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read request body failed", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	isStream := upstreamPath == chatPath && requestWantsStream(body)
	selected, err := g.acquireKey()
	if err != nil {
		status := http.StatusTooManyRequests
		if errors.Is(err, errUpstreamNotConfigured) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), status)
		return
	}
	defer g.releaseKey(selected.key.ID)

	capture := store.Capture{
		KeyID:         selected.key.ID,
		Time:          time.Now().UTC(),
		RequestMethod: r.Method,
		RequestURL:    joinURL(selected.baseURL, upstreamPath),
		RequestHeader: redactHeader(r.Header),
		RequestBody:   string(body),
	}
	progress := newCaptureProgressRecorder(nil)
	emitRequestReady(progress.emit, selected)
	start := time.Now()

	status, respHeader, respBody, forwardErr := g.forward(r.Context(), selected, r.Method, upstreamPath, body, isStream, w, progress.emit)
	capture.DurationMS = time.Since(start).Milliseconds()
	capture.StatusCode = status
	capture.ResponseHeader = redactHeader(respHeader)
	capture.ResponseBody = string(respBody)
	if forwardErr != nil {
		capture.Error = forwardErr.Error()
	}
	capture.NetworkEvents = progress.events()
	if upstreamPath == chatPath {
		_ = g.store.SaveCapture(capture)
		_ = g.store.RecordMetric(selected.key.ID, store.MetricSample{
			Time:        capture.Time,
			FirstByteMS: firstByteElapsedMS(capture.NetworkEvents),
			Concurrency: g.totalConcurrency(),
			Error:       forwardErr != nil || status >= 400,
		})

		if forwardErr != nil {
			_, _ = g.store.RecordFailure(selected.key.ID, forwardErr.Error(), false)
			return
		}
		if status >= 400 {
			msg := truncate(string(respBody), 1000)
			_, _ = g.store.RecordFailure(selected.key.ID, msg, looksBalanceDepleted(status, respBody))
			return
		}
		_ = g.store.RecordSuccess(selected.key.ID)
	}
}

func routeRequest(r *http.Request) (string, string, error) {
	switch r.URL.Path {
	case chatPath:
		return chatPath, http.MethodPost, nil
	case modelsPath:
		return modelsPath, http.MethodGet, nil
	default:
		return "", "", fmt.Errorf("not found")
	}
}

func (g *Gateway) CurrentConcurrency() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string]int, len(g.inflight))
	for k, v := range g.inflight {
		out[k] = v
	}
	return out
}

func (g *Gateway) totalConcurrency() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	total := 0
	for _, v := range g.inflight {
		total += v
	}
	return total
}

func (g *Gateway) acquireKey() (selectedKey, error) {
	keys, err := g.store.ListKeys()
	if err != nil {
		return selectedKey{}, err
	}
	cfg, _ := g.store.GlobalConfig()

	g.mu.Lock()
	defer g.mu.Unlock()

	candidates := make([]store.Key, 0, len(keys))
	for _, key := range keys {
		if !key.Enabled || key.Paused || key.BalanceDepleted {
			continue
		}
		if g.inflight[key.ID] >= key.MaxConcurrency {
			continue
		}
		candidates = append(candidates, key)
	}
	if len(candidates) == 0 {
		return selectedKey{}, errNoAvailableKey
	}

	idx := g.roundRobin % len(candidates)
	key := candidates[idx]
	g.roundRobin++
	g.inflight[key.ID]++

	baseURL := strings.TrimSpace(cfg.UpstreamBaseURL)
	if strings.TrimSpace(key.BaseURL) != "" {
		baseURL = strings.TrimSpace(key.BaseURL)
	}
	if baseURL == "" {
		g.inflight[key.ID]--
		return selectedKey{}, errUpstreamNotConfigured
	}

	proxyURL := strings.TrimSpace(cfg.DefaultProxyURL)
	if strings.TrimSpace(key.ProxyURL) != "" {
		proxyURL = strings.TrimSpace(key.ProxyURL)
	}
	return selectedKey{key: key, baseURL: baseURL, proxyURL: proxyURL}, nil
}

func (g *Gateway) releaseKey(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inflight[id] > 0 {
		g.inflight[id]--
	}
}

func (g *Gateway) forward(ctx context.Context, selected selectedKey, method string, upstreamPath string, body []byte, isStream bool, w http.ResponseWriter, progress func(TestProgressEvent)) (int, http.Header, []byte, error) {
	if !isStream {
		status, header, respBody, err := g.doBufferedWithProgress(ctx, selected, method, upstreamPath, body, progress)
		if err != nil {
			http.Error(w, "upstream request failed", http.StatusBadGateway)
			return 0, nil, nil, err
		}
		copyResponseHeader(w.Header(), header)
		w.WriteHeader(status)
		if len(respBody) > 0 {
			_, _ = w.Write(respBody)
		}
		return status, header, respBody, nil
	}

	client, err := g.clientForProxy(selected.proxyURL)
	if err != nil {
		emitProgress(progress, TestProgressEvent{Step: "error", Message: "代理配置无效", Error: err.Error()})
		http.Error(w, "invalid proxy URL", http.StatusBadGateway)
		return 0, nil, nil, err
	}

	var lastErr error
	for attempt := 1; attempt <= upstreamMaxAttempts; attempt++ {
		emitProgress(progress, TestProgressEvent{Step: "attempt", Message: fmt.Sprintf("开始第 %d 次请求上游", attempt), Attempt: attempt})
		req, err := newUpstreamRequest(ctx, selected, method, upstreamPath, body, isStream)
		if err != nil {
			emitProgress(progress, TestProgressEvent{Step: "error", Message: "构建上游请求失败", Attempt: attempt, Error: err.Error()})
			http.Error(w, "build upstream request failed", http.StatusBadGateway)
			return 0, nil, nil, err
		}
		if trace := traceProgress(progress, attempt, selected.proxyURL); trace != nil {
			req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			emitProgress(progress, TestProgressEvent{Step: "request_error", Message: "请求上游出错", Attempt: attempt, Error: err.Error()})
			if !shouldRetryRequest(ctx, attempt, 0, err) {
				http.Error(w, "upstream request failed", http.StatusBadGateway)
				return 0, nil, nil, err
			}
			emitProgress(progress, TestProgressEvent{Step: "retry", Message: "准备重试", Attempt: attempt})
			if err := waitRetryDelay(ctx, attempt); err != nil {
				http.Error(w, "upstream request failed", http.StatusBadGateway)
				return 0, nil, nil, err
			}
			continue
		}

		if shouldRetryRequest(ctx, attempt, resp.StatusCode, nil) {
			emitProgress(progress, TestProgressEvent{Step: "retry_status", Message: fmt.Sprintf("上游返回 %d，准备重试", resp.StatusCode), Attempt: attempt})
			drainAndClose(resp.Body)
			if err := waitRetryDelay(ctx, attempt); err != nil {
				http.Error(w, "upstream request failed", http.StatusBadGateway)
				return 0, nil, nil, err
			}
			continue
		}

		defer resp.Body.Close()
		copyResponseHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)

		status, header, respBody, readErr := streamResponse(w, resp)
		if readErr != nil {
			emitProgress(progress, TestProgressEvent{Step: "read_error", Message: "读取流式响应失败", Attempt: attempt, Error: readErr.Error()})
		} else {
			emitProgress(progress, TestProgressEvent{Step: "response_read", Message: fmt.Sprintf("流式响应读取完成，状态码 %d", resp.StatusCode), Attempt: attempt})
		}
		return status, header, respBody, readErr
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("upstream request failed after %d attempts", upstreamMaxAttempts)
	}
	http.Error(w, "upstream request failed", http.StatusBadGateway)
	return 0, nil, nil, lastErr
}

func (g *Gateway) ListModelsForKey(ctx context.Context, key store.Key, cfg store.GlobalConfig) (ModelListResult, error) {
	selected, err := selectedForKey(key, cfg)
	if err != nil {
		return ModelListResult{}, err
	}
	status, _, body, err := g.doBuffered(ctx, selected, http.MethodGet, modelsPath, nil)
	if err != nil {
		return ModelListResult{}, err
	}
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		raw = string(body)
	}
	if status >= 400 {
		return ModelListResult{Raw: raw}, fmt.Errorf("upstream returned %d: %s", status, truncate(string(body), 400))
	}
	return ModelListResult{Models: extractModelIDs(raw), Raw: raw}, nil
}

func (g *Gateway) TestKey(ctx context.Context, key store.Key, cfg store.GlobalConfig, model string, prompt string) (UpstreamTestResult, error) {
	return g.TestKeyWithProgress(ctx, key, cfg, model, prompt, nil)
}

func (g *Gateway) TestKeyWithProgress(ctx context.Context, key store.Key, cfg store.GlobalConfig, model string, prompt string, progress func(TestProgressEvent)) (UpstreamTestResult, error) {
	emitProgress(progress, TestProgressEvent{Step: "prepare", Message: "准备测试请求"})
	selected, err := selectedForKey(key, cfg)
	if err != nil {
		emitProgress(progress, TestProgressEvent{Step: "error", Message: "配置不可用", Error: err.Error()})
		return UpstreamTestResult{}, err
	}
	model = strings.TrimSpace(model)
	if model == "" {
		emitProgress(progress, TestProgressEvent{Step: "error", Message: "模型不能为空", Error: "model is required"})
		return UpstreamTestResult{}, fmt.Errorf("model is required")
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		prompt = "ping"
	}
	body, err := json.Marshal(map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	})
	if err != nil {
		emitProgress(progress, TestProgressEvent{Step: "error", Message: "构建请求失败", Error: err.Error()})
		return UpstreamTestResult{}, err
	}
	captureProgress := newCaptureProgressRecorder(progress)
	emitRequestReady(captureProgress.emit, selected)

	capture := store.Capture{
		KeyID:         key.ID,
		Time:          time.Now().UTC(),
		RequestMethod: http.MethodPost,
		RequestURL:    joinURL(selected.baseURL, chatPath),
		RequestBody:   string(body),
	}
	start := time.Now()
	status, header, respBody, forwardErr := g.doBufferedWithProgress(ctx, selected, http.MethodPost, chatPath, body, captureProgress.emit)
	capture.DurationMS = time.Since(start).Milliseconds()
	capture.StatusCode = status
	capture.ResponseHeader = redactHeader(header)
	capture.ResponseBody = string(respBody)
	if forwardErr != nil {
		capture.Error = forwardErr.Error()
		captureProgress.emit(TestProgressEvent{Step: "upstream_error", Message: "上游请求失败", DurationMS: capture.DurationMS, Error: forwardErr.Error()})
	}
	capture.NetworkEvents = captureProgress.events()
	if g.store != nil {
		emitProgress(progress, TestProgressEvent{Step: "record", Message: "写入测试结果和 Key 状态"})
		_ = g.store.SaveCapture(capture)
		if forwardErr != nil {
			_, _ = g.store.RecordFailure(key.ID, forwardErr.Error(), false)
		} else if status >= 400 {
			_, _ = g.store.RecordFailure(key.ID, truncate(string(respBody), 1000), looksBalanceDepleted(status, respBody))
		} else {
			_ = g.store.RecordSuccess(key.ID)
		}
	}
	result := UpstreamTestResult{
		StatusCode:     status,
		DurationMS:     capture.DurationMS,
		ResponseHeader: redactHeader(header),
		ResponseBody:   string(respBody),
	}
	if forwardErr != nil {
		result.Error = forwardErr.Error()
		return result, forwardErr
	}
	emitProgress(progress, TestProgressEvent{Step: "success", Message: "测试请求完成", DurationMS: capture.DurationMS})
	return result, nil
}

func (g *Gateway) doBuffered(ctx context.Context, selected selectedKey, method string, upstreamPath string, body []byte) (int, http.Header, []byte, error) {
	return g.doBufferedWithProgress(ctx, selected, method, upstreamPath, body, nil)
}

func (g *Gateway) doBufferedWithProgress(ctx context.Context, selected selectedKey, method string, upstreamPath string, body []byte, progress func(TestProgressEvent)) (int, http.Header, []byte, error) {
	client, err := g.clientForProxy(selected.proxyURL)
	if err != nil {
		emitProgress(progress, TestProgressEvent{Step: "error", Message: "代理配置无效", Error: err.Error()})
		return 0, nil, nil, err
	}

	var lastErr error
	for attempt := 1; attempt <= upstreamMaxAttempts; attempt++ {
		emitProgress(progress, TestProgressEvent{Step: "attempt", Message: fmt.Sprintf("开始第 %d 次请求上游", attempt), Attempt: attempt})
		req, err := newUpstreamRequest(ctx, selected, method, upstreamPath, body, false)
		if err != nil {
			emitProgress(progress, TestProgressEvent{Step: "error", Message: "构建上游请求失败", Attempt: attempt, Error: err.Error()})
			return 0, nil, nil, err
		}
		if trace := traceProgress(progress, attempt, selected.proxyURL); trace != nil {
			req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			emitProgress(progress, TestProgressEvent{Step: "request_error", Message: "请求上游出错", Attempt: attempt, Error: err.Error()})
			if !shouldRetryRequest(ctx, attempt, 0, err) {
				return 0, nil, nil, err
			}
			emitProgress(progress, TestProgressEvent{Step: "retry", Message: "准备重试", Attempt: attempt})
			if err := waitRetryDelay(ctx, attempt); err != nil {
				return 0, nil, nil, err
			}
			continue
		}

		if shouldRetryRequest(ctx, attempt, resp.StatusCode, nil) {
			emitProgress(progress, TestProgressEvent{Step: "retry_status", Message: fmt.Sprintf("上游返回 %d，准备重试", resp.StatusCode), Attempt: attempt})
			drainAndClose(resp.Body)
			if err := waitRetryDelay(ctx, attempt); err != nil {
				return 0, nil, nil, err
			}
			continue
		}

		respBody, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			emitProgress(progress, TestProgressEvent{Step: "read_error", Message: "读取响应失败", Attempt: attempt, Error: readErr.Error()})
		} else {
			emitProgress(progress, TestProgressEvent{Step: "response_read", Message: fmt.Sprintf("响应读取完成，状态码 %d", resp.StatusCode), Attempt: attempt})
		}
		return resp.StatusCode, resp.Header, respBody, readErr
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("upstream request failed after %d attempts", upstreamMaxAttempts)
	}
	return 0, nil, nil, lastErr
}

func emitProgress(progress func(TestProgressEvent), event TestProgressEvent) {
	if progress != nil {
		progress(event)
	}
}

func emitRequestReady(progress func(TestProgressEvent), selected selectedKey) {
	if selected.usesProxy() {
		emitProgress(progress, TestProgressEvent{Step: "proxy_config", Message: "使用代理：" + proxyDisplayURL(selected.proxyURL)})
		emitProgress(progress, TestProgressEvent{Step: "request_ready", Message: "请求已构建，准备通过代理连接上游"})
		return
	}
	emitProgress(progress, TestProgressEvent{Step: "request_ready", Message: "请求已构建，准备直连上游"})
}

type captureProgressRecorder struct {
	external func(TestProgressEvent)
	start    time.Time
	items    []store.CaptureNetworkEvent
}

func newCaptureProgressRecorder(external func(TestProgressEvent)) *captureProgressRecorder {
	return &captureProgressRecorder{external: external, start: time.Now()}
}

func (r *captureProgressRecorder) emit(event TestProgressEvent) {
	event.ElapsedMS = time.Since(r.start).Milliseconds()
	emitProgress(r.external, event)
	if !shouldCaptureNetworkEvent(event) {
		return
	}
	r.items = append(r.items, store.CaptureNetworkEvent{
		Step:       event.Step,
		Message:    event.Message,
		Attempt:    event.Attempt,
		DurationMS: event.DurationMS,
		ElapsedMS:  event.ElapsedMS,
		Error:      event.Error,
	})
}

func (r *captureProgressRecorder) events() []store.CaptureNetworkEvent {
	return append([]store.CaptureNetworkEvent(nil), r.items...)
}

func firstByteElapsedMS(events []store.CaptureNetworkEvent) *int64 {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Step != "first_byte" {
			continue
		}
		value := events[i].ElapsedMS
		return &value
	}
	return nil
}

func shouldCaptureNetworkEvent(event TestProgressEvent) bool {
	switch event.Step {
	case "proxy_config", "request_ready", "attempt", "dns_start", "dns_done",
		"connect_start", "connect_done", "got_conn", "proxy_tunnel",
		"tls_start", "tls_done", "wrote_request", "first_byte",
		"request_error", "retry", "retry_status", "read_error",
		"response_read", "upstream_error":
		return true
	default:
		return false
	}
}

func proxyDisplayURL(proxyValue string) string {
	proxyValue = strings.TrimSpace(proxyValue)
	if proxyValue == "" {
		return ""
	}
	u, err := url.Parse(proxyValue)
	if err != nil || u.Host == "" {
		return "已配置代理（地址格式待校验）"
	}
	if u.Scheme == "" {
		return u.Host
	}
	return u.Scheme + "://" + u.Host
}

func traceProgress(progress func(TestProgressEvent), attempt int, proxyURL string) *httptrace.ClientTrace {
	if progress == nil {
		return nil
	}
	hasProxy := strings.TrimSpace(proxyURL) != ""
	return &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			target := "上游域名"
			if hasProxy {
				target = "代理域名"
			}
			emitProgress(progress, TestProgressEvent{Step: "dns_start", Message: "开始解析" + target + "：" + info.Host, Attempt: attempt})
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			target := "上游 DNS"
			if hasProxy {
				target = "代理 DNS"
			}
			if info.Err != nil {
				emitProgress(progress, TestProgressEvent{Step: "dns_done", Message: target + " 解析失败", Attempt: attempt, Error: info.Err.Error()})
				return
			}
			emitProgress(progress, TestProgressEvent{Step: "dns_done", Message: target + " 解析完成", Attempt: attempt})
		},
		ConnectStart: func(network, addr string) {
			target := "上游服务器"
			if hasProxy {
				target = "代理服务器"
			}
			emitProgress(progress, TestProgressEvent{Step: "connect_start", Message: "开始连接" + target + "：" + addr, Attempt: attempt})
		},
		ConnectDone: func(network, addr string, err error) {
			target := "上游服务器"
			if hasProxy {
				target = "代理服务器"
			}
			if err != nil {
				emitProgress(progress, TestProgressEvent{Step: "connect_done", Message: target + "连接失败：" + addr, Attempt: attempt, Error: err.Error()})
				return
			}
			emitProgress(progress, TestProgressEvent{Step: "connect_done", Message: target + "连接完成：" + addr, Attempt: attempt})
		},
		GotConn: func(info httptrace.GotConnInfo) {
			target := "上游连接"
			if hasProxy {
				target = "代理连接"
			}
			if info.Reused {
				emitProgress(progress, TestProgressEvent{Step: "got_conn", Message: "复用已有" + target, Attempt: attempt})
				return
			}
			emitProgress(progress, TestProgressEvent{Step: "got_conn", Message: "已建立新的" + target, Attempt: attempt})
		},
		TLSHandshakeStart: func() {
			if hasProxy {
				emitProgress(progress, TestProgressEvent{Step: "proxy_tunnel", Message: "代理隧道已建立，准备与上游 TLS 握手", Attempt: attempt})
			}
			emitProgress(progress, TestProgressEvent{Step: "tls_start", Message: "开始上游 TLS 握手", Attempt: attempt})
		},
		TLSHandshakeDone: func(state tls.ConnectionState, err error) {
			if err != nil {
				emitProgress(progress, TestProgressEvent{Step: "tls_done", Message: "上游 TLS 握手失败", Attempt: attempt, Error: err.Error()})
				return
			}
			protocol := state.NegotiatedProtocol
			if protocol == "" {
				protocol = "http/1.1"
			}
			emitProgress(progress, TestProgressEvent{Step: "tls_done", Message: "上游 TLS 握手完成，协议：" + protocol, Attempt: attempt})
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err != nil {
				emitProgress(progress, TestProgressEvent{Step: "wrote_request", Message: "请求发送失败", Attempt: attempt, Error: info.Err.Error()})
				return
			}
			emitProgress(progress, TestProgressEvent{Step: "wrote_request", Message: "请求已发送，等待上游响应", Attempt: attempt})
		},
		GotFirstResponseByte: func() {
			emitProgress(progress, TestProgressEvent{Step: "first_byte", Message: "收到上游首字节，开始读取响应", Attempt: attempt})
		},
	}
}

func selectedForKey(key store.Key, cfg store.GlobalConfig) (selectedKey, error) {
	baseURL := strings.TrimSpace(cfg.UpstreamBaseURL)
	if strings.TrimSpace(key.BaseURL) != "" {
		baseURL = strings.TrimSpace(key.BaseURL)
	}
	if baseURL == "" {
		return selectedKey{}, errUpstreamNotConfigured
	}
	proxyURL := strings.TrimSpace(cfg.DefaultProxyURL)
	if strings.TrimSpace(key.ProxyURL) != "" {
		proxyURL = strings.TrimSpace(key.ProxyURL)
	}
	return selectedKey{key: key, baseURL: baseURL, proxyURL: proxyURL}, nil
}

func (g *Gateway) clientForProxy(proxyURL string) (*http.Client, error) {
	key := strings.TrimSpace(proxyURL)
	g.mu.Lock()
	client := g.clients[key]
	g.mu.Unlock()
	if client != nil {
		return client, nil
	}

	client, err := g.newClient(key)
	if err != nil {
		return nil, err
	}

	g.mu.Lock()
	if existing := g.clients[key]; existing != nil {
		g.mu.Unlock()
		return existing, nil
	}
	g.clients[key] = client
	g.mu.Unlock()
	return client, nil
}

func newUpstreamRequest(ctx context.Context, selected selectedKey, method string, upstreamPath string, body []byte, isStream bool) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, joinURL(selected.baseURL, upstreamPath), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+selected.key.APIKey)
	req.Header.Set("Accept", "application/json")
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if isStream {
		req.Header.Set("Accept", "text/event-stream")
	}
	return req, nil
}

func shouldRetryRequest(ctx context.Context, attempt int, status int, err error) bool {
	if attempt >= upstreamMaxAttempts || ctx.Err() != nil {
		return false
	}
	if err != nil {
		return true
	}
	return status == http.StatusRequestTimeout ||
		status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable ||
		status == http.StatusGatewayTimeout
}

func waitRetryDelay(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt) * upstreamRetryBaseDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 64*1024))
	_ = body.Close()
}

func streamResponse(w http.ResponseWriter, resp *http.Response) (int, http.Header, []byte, error) {
	flusher, _ := w.(http.Flusher)
	buf := make([]byte, 32*1024)
	var captured bytes.Buffer
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			captured.Write(chunk)
			if _, err := w.Write(chunk); err != nil {
				return resp.StatusCode, resp.Header, captured.Bytes(), err
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if readErr == io.EOF {
			return resp.StatusCode, resp.Header, captured.Bytes(), nil
		}
		if readErr != nil {
			return resp.StatusCode, resp.Header, captured.Bytes(), readErr
		}
	}
}

func requestWantsStream(body []byte) bool {
	var payload struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &payload) == nil && payload.Stream
}

func extractModelIDs(raw any) []string {
	root, ok := raw.(map[string]any)
	if !ok {
		return nil
	}
	data, ok := root["data"].([]any)
	if !ok {
		data, _ = root["models"].([]any)
	}
	models := make([]string, 0, len(data))
	for _, item := range data {
		switch value := item.(type) {
		case string:
			if value != "" {
				models = append(models, value)
			}
		case map[string]any:
			if id, _ := value["id"].(string); id != "" {
				models = append(models, id)
			}
		}
	}
	return models
}

func httpClientForProxy(proxyValue string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Some residential proxies break HTTP/2 streams after CONNECT. NVIDIA works
	// over HTTP/1.1, so keep upstream transport conservative and proxy-friendly.
	transport.ForceAttemptHTTP2 = false
	transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{NextProtos: []string{"http/1.1"}}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	transport.DialContext = (&net.Dialer{
		Timeout:   upstreamDialTimeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.TLSHandshakeTimeout = upstreamTLSHandshakeLimit
	transport.MaxIdleConns = 200
	transport.MaxIdleConnsPerHost = 50
	transport.IdleConnTimeout = 90 * time.Second
	if strings.TrimSpace(proxyValue) != "" {
		u, err := url.Parse(proxyValue)
		if err != nil {
			return nil, err
		}
		transport.Proxy = http.ProxyURL(u)
	}
	return &http.Client{Transport: transport}, nil
}

func joinURL(base, path string) string {
	return strings.TrimRight(base, "/") + path
}

func copyResponseHeader(dst, src http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

func redactHeader(src http.Header) map[string][]string {
	out := make(map[string][]string, len(src))
	for k, values := range src {
		copied := append([]string(nil), values...)
		if strings.EqualFold(k, "Authorization") || strings.EqualFold(k, "Proxy-Authorization") || strings.EqualFold(k, "X-Api-Key") {
			for i := range copied {
				copied[i] = "[redacted]"
			}
		}
		out[k] = copied
	}
	return out
}

func looksBalanceDepleted(status int, body []byte) bool {
	if status == http.StatusPaymentRequired {
		return true
	}
	text := strings.ToLower(string(body))
	keywords := []string{
		"insufficient", "balance", "quota", "credit", "billing", "余额", "额度", "欠费", "用尽", "不足",
	}
	for _, kw := range keywords {
		if strings.Contains(text, kw) {
			return true
		}
	}
	return false
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
