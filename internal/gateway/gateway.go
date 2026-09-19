package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"api-fucker/internal/store"
)

const (
	chatPath                  = "/v1/chat/completions"
	imageGenerationsPath      = "/v1/images/generations"
	modelsPath                = "/v1/models"
	imageCaptureResponseLimit = 64 * 1024
	upstreamMaxAttempts       = 3
	upstreamRetryBaseDelay    = 300 * time.Millisecond
	upstreamDialTimeout       = 30 * time.Second
	upstreamTLSHandshakeLimit = 30 * time.Second
)

var (
	errNoAvailableKey        = errors.New("no available upstream key")
	errNoAvailableProxy      = errors.New("no available proxy node")
	errUpstreamNotConfigured = errors.New("upstream base URL is not configured")
	errGlobalQueueFull       = errors.New("apiFucker超过最大排队数量")
	errGlobalQueueTimeout    = errors.New("apiFucker排队等待超时")
)

type Gateway struct {
	store *store.Store

	mu              sync.Mutex
	lastKeyID       string
	lastMinuteSweep time.Time
	proxyRoundRobin int
	inflight        map[string]int
	proxyInflight   map[string]int
	keyMinute       map[string]*minuteCounter
	clients         map[string]*http.Client
	newClient       func(proxyURL string) (*http.Client, error)
	proxyNodeToken  string

	queueMu       sync.Mutex
	globalRunning int
	globalQueued  int
	queueNotify   chan struct{}
	queuePeak     int32

	launchMu   sync.Mutex
	nextLaunch time.Time
}

// minuteCounter is a per-key sliding-window log: it stores the timestamps
// of every request admitted in the trailing minute. A request is "in window"
// iff its timestamp is within the last 60s of `now`. Prune drops the expired
// prefix lazily so reads are accurate without shifting on every admission.
type minuteCounter struct {
	hits []time.Time
	head int
}

const minuteWindow = time.Minute

// prune removes timestamps older than `now - minuteWindow` from the front
// of hits. Since hits is appended in monotonically increasing time order,
// the expired entries are always a prefix.
func (mc *minuteCounter) prune(now time.Time) {
	cutoff := now.Add(-minuteWindow)
	for mc.head < len(mc.hits) && !mc.hits[mc.head].After(cutoff) {
		mc.head++
	}
	if mc.head == len(mc.hits) {
		mc.hits = nil
		mc.head = 0
	} else if mc.head > 0 && mc.head >= len(mc.hits)/2 {
		// Release peak-sized backing arrays as traffic subsides, amortizing copies.
		remaining := make([]time.Time, len(mc.hits)-mc.head)
		copy(remaining, mc.hits[mc.head:])
		mc.hits = remaining
		mc.head = 0
	}
}

func (mc *minuteCounter) count() int {
	return len(mc.hits) - mc.head
}

type selectedKey struct {
	key         store.Key
	baseURL     string
	proxyURL    string
	proxyNodeID string
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

func New(st *store.Store, _ *http.Client, proxyNodeToken ...string) *Gateway {
	token := ""
	if len(proxyNodeToken) > 0 {
		token = proxyNodeToken[0]
	}
	return &Gateway{
		store:          st,
		inflight:       make(map[string]int),
		proxyInflight:  make(map[string]int),
		keyMinute:      make(map[string]*minuteCounter),
		clients:        make(map[string]*http.Client),
		newClient:      httpClientForProxy,
		proxyNodeToken: token,
		queueNotify:    make(chan struct{}),
	}
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
	isManagedGeneration := isManagedGenerationPath(upstreamPath)
	if isManagedGeneration {
		cfg, _ := g.store.GlobalConfig()
		releaseGlobal, err := g.acquireGlobalSlot(r.Context(), cfg)
		if err != nil {
			status := http.StatusGatewayTimeout
			if errors.Is(err, errGlobalQueueFull) {
				status = http.StatusTooManyRequests
			}
			http.Error(w, err.Error(), status)
			return
		}
		defer releaseGlobal()
		if err := g.waitForLaunchSlot(r.Context(), cfg.LaunchIntervalMS); err != nil {
			http.Error(w, err.Error(), http.StatusGatewayTimeout)
			return
		}
	}

	selected, err := g.acquireKey()
	if err != nil {
		status := http.StatusTooManyRequests
		if errors.Is(err, errUpstreamNotConfigured) || errors.Is(err, errNoAvailableProxy) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), status)
		return
	}
	defer g.releaseSelected(selected)

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
	g.recordUpstreamResult(selected.key.ID, status, respBody, forwardErr, capture.DurationMS, isManagedGeneration)
	capture.StatusCode = status
	capture.ResponseHeader = redactHeader(respHeader)
	capture.ResponseBody = captureResponseBody(upstreamPath, respBody)
	if forwardErr != nil && !errors.Is(forwardErr, context.Canceled) {
		capture.Error = forwardErr.Error()
	}
	capture.RequestHeader["Authorization"] = []string{"Bearer " + selected.key.APIKey}
	capture.NetworkEvents = progress.events()
	if isManagedGeneration {
		_ = g.store.SaveCapture(capture)
		_ = g.store.RecordMetric(selected.key.ID, store.MetricSample{
			Time:        capture.Time,
			FirstByteMS: firstByteElapsedMS(capture.NetworkEvents),
			Concurrency: g.totalConcurrency(),
			QueueDepth:  g.consumeQueuePeak(),
			Error:       status == http.StatusTooManyRequests || (!errors.Is(forwardErr, context.Canceled) && (forwardErr != nil || status >= 400)),
		})
	}
}

// Called exactly once per logical upstream request, before saving large captures.
func (g *Gateway) recordUpstreamResult(id string, status int, body []byte, err error, durationMS int64, generation bool) {
	if g.store == nil {
		return
	}
	if status == http.StatusTooManyRequests {
		message := truncate(string(body), 1000)
		if message == "" {
			message = "upstream returned HTTP 429"
		}
		_, _ = g.store.RecordRateLimit(id, message, durationMS)
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	if !generation {
		_ = g.store.RecordNon429(id, status)
		return
	}
	if err != nil {
		_, _ = g.store.RecordFailure(id, err.Error(), false, durationMS)
		return
	}
	if status >= 400 {
		_, _ = g.store.RecordFailure(id, truncate(string(body), 1000), looksBalanceDepleted(status, body), durationMS)
		return
	}
	_ = g.store.RecordSuccess(id, durationMS)
}

func (g *Gateway) acquireGlobalSlot(ctx context.Context, cfg store.GlobalConfig) (func(), error) {
	maxConcurrency := cfg.GlobalMaxConcurrency
	if maxConcurrency <= 0 {
		return func() {}, nil
	}
	queueTimeout := time.Duration(cfg.QueueTimeoutMS) * time.Millisecond
	if queueTimeout <= 0 {
		queueTimeout = 30 * time.Second
	}

	g.queueMu.Lock()
	if g.globalRunning < maxConcurrency {
		g.globalRunning++
		g.queueMu.Unlock()
		return g.releaseGlobalSlot, nil
	}
	if cfg.MaxQueueSize <= 0 || g.globalQueued >= cfg.MaxQueueSize {
		g.queueMu.Unlock()
		return nil, errGlobalQueueFull
	}
	g.globalQueued++
	g.observeQueuePeakLocked()
	timer := time.NewTimer(queueTimeout)
	defer timer.Stop()

	for {
		notify := g.queueNotify
		g.queueMu.Unlock()
		select {
		case <-ctx.Done():
			g.queueMu.Lock()
			g.globalQueued--
			g.signalGlobalQueueLocked()
			g.queueMu.Unlock()
			return nil, errGlobalQueueTimeout
		case <-timer.C:
			g.queueMu.Lock()
			g.globalQueued--
			g.signalGlobalQueueLocked()
			g.queueMu.Unlock()
			return nil, errGlobalQueueTimeout
		case <-notify:
			g.queueMu.Lock()
			if g.globalRunning < maxConcurrency {
				g.globalQueued--
				g.globalRunning++
				g.queueMu.Unlock()
				return g.releaseGlobalSlot, nil
			}
		}
	}
}

func (g *Gateway) releaseGlobalSlot() {
	g.queueMu.Lock()
	defer g.queueMu.Unlock()
	if g.globalRunning > 0 {
		g.globalRunning--
	}
	g.signalGlobalQueueLocked()
}

func (g *Gateway) signalGlobalQueueLocked() {
	close(g.queueNotify)
	g.queueNotify = make(chan struct{})
}

// observeQueuePeakLocked records the current queue depth as a new peak if it
// exceeds the value seen since the last sample read. Caller must hold queueMu.
func (g *Gateway) observeQueuePeakLocked() {
	current := int32(g.globalQueued)
	for {
		prev := atomic.LoadInt32(&g.queuePeak)
		if current <= prev {
			return
		}
		if atomic.CompareAndSwapInt32(&g.queuePeak, prev, current) {
			return
		}
	}
}

// QueueDepth returns the current number of requests waiting in the global
// queue. This is a live snapshot used by the admin metrics endpoint.
func (g *Gateway) QueueDepth() int {
	g.queueMu.Lock()
	defer g.queueMu.Unlock()
	return g.globalQueued
}

// consumeQueuePeak returns the highest queue depth seen since the previous
// call and resets the counter to zero. Used per RecordMetric sample.
func (g *Gateway) consumeQueuePeak() int {
	return int(atomic.SwapInt32(&g.queuePeak, 0))
}

// waitForLaunchSlot enforces a minimum spacing between successive upstream
// launches so a sudden burst (e.g. 40 simultaneous clients hitting a freshly
// raised concurrency cap) cannot land on the relay all in the same instant.
// Each caller atomically reserves the next slot on the timeline, then sleeps
// until that wall-clock instant. Returns ctx.Err() if cancelled while waiting.
// intervalMs <= 0 disables the pacer.
func (g *Gateway) waitForLaunchSlot(ctx context.Context, intervalMs int) error {
	if intervalMs <= 0 {
		return nil
	}
	interval := time.Duration(intervalMs) * time.Millisecond

	g.launchMu.Lock()
	now := time.Now()
	var wait time.Duration
	if g.nextLaunch.Before(now) {
		g.nextLaunch = now.Add(interval)
	} else {
		wait = g.nextLaunch.Sub(now)
		g.nextLaunch = g.nextLaunch.Add(interval)
	}
	g.launchMu.Unlock()

	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// KeyMinuteUsage returns a snapshot of each key's request count in the
// trailing 60s sliding window. Each entry is pruned of expired hits before
// reading, and empty entries are dropped from the map.
func (g *Gateway) KeyMinuteUsage() map[string]int {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	out := make(map[string]int, len(g.keyMinute))
	for id := range g.keyMinute {
		if count := g.keyMinuteCountLocked(id, now); count > 0 {
			out[id] = count
		}
	}
	return out
}

// KeyMinuteUsageForKey reads only this key's in-memory rolling window.
func (g *Gateway) KeyMinuteUsageForKey(id string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.keyMinuteCountLocked(id, time.Now())
}

func (g *Gateway) keyMinuteCountLocked(id string, now time.Time) int {
	mc := g.keyMinute[id]
	if mc == nil {
		delete(g.keyMinute, id)
		return 0
	}
	mc.prune(now)
	if mc.count() == 0 {
		delete(g.keyMinute, id)
		return 0
	}
	return mc.count()
}

func routeRequest(r *http.Request) (string, string, error) {
	switch r.URL.Path {
	case chatPath:
		return chatPath, http.MethodPost, nil
	case imageGenerationsPath:
		return imageGenerationsPath, http.MethodPost, nil
	case modelsPath:
		return modelsPath, http.MethodGet, nil
	default:
		return "", "", fmt.Errorf("not found")
	}
}

func isManagedGenerationPath(path string) bool {
	return path == chatPath || path == imageGenerationsPath
}

func captureResponseBody(path string, body []byte) string {
	if path != imageGenerationsPath || len(body) <= imageCaptureResponseLimit {
		return string(body)
	}
	return string(body[:imageCaptureResponseLimit]) + fmt.Sprintf("\n...[truncated: original response body %d bytes]", len(body))
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

// pruneKeyMinuteLocked sweeps at most once per minute. ListKeys already
// supplies keys sorted by ID, so cleanup needs no additional key-set allocation.
// Individual candidates are pruned lazily on every selection.
func (g *Gateway) pruneKeyMinuteLocked(keys []store.Key, now time.Time) {
	if !g.lastMinuteSweep.IsZero() && now.Sub(g.lastMinuteSweep) < minuteWindow {
		return
	}
	g.lastMinuteSweep = now
	for id := range g.keyMinute {
		idx := sort.Search(len(keys), func(i int) bool { return keys[i].ID >= id })
		if idx == len(keys) || keys[idx].ID != id {
			delete(g.keyMinute, id)
			continue
		}
		g.keyMinuteCountLocked(id, now)
	}
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
	proxyNodes, _ := g.store.ListProxyNodes(time.Now().UTC())

	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	rpmLimit := cfg.KeyMaxRequestsPerMinute
	g.pruneKeyMinuteLocked(keys, now)

	// Keep the cursor in the stable full key order, not in a filtered list.
	start := sort.Search(len(keys), func(i int) bool { return keys[i].ID > g.lastKeyID })
	var key store.Key
	found := false
	for offset := 0; offset < len(keys); offset++ {
		candidate := keys[(start+offset)%len(keys)]
		if !candidate.Enabled || candidate.ManualPaused || candidate.Paused || candidate.BalanceDepleted || candidate.IsRateLimited(now) {
			continue
		}
		if g.inflight[candidate.ID] >= candidate.MaxConcurrency {
			continue
		}
		count := g.keyMinuteCountLocked(candidate.ID, now)
		if rpmLimit > 0 && count >= rpmLimit {
			continue
		}
		key = candidate
		found = true
		break
	}
	if !found {
		return selectedKey{}, errNoAvailableKey
	}
	g.lastKeyID = key.ID
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
	proxyNodeID := ""
	if cfg.ProxyPoolEnabled {
		node, err := g.selectProxyNodeLocked(key.ID, cfg.ProxyMode, proxyNodes)
		if err != nil {
			g.inflight[key.ID]--
			return selectedKey{}, err
		}
		proxyURL = proxyURLWithToken(node.ProxyURL, g.proxyNodeToken)
		proxyNodeID = node.ID
		g.proxyInflight[node.ID]++
	}

	// Count logical admissions even when the configured RPM limit is disabled.
	mc := g.keyMinute[key.ID]
	if mc == nil {
		mc = &minuteCounter{}
		g.keyMinute[key.ID] = mc
	}
	mc.hits = append(mc.hits, now)
	return selectedKey{key: key, baseURL: baseURL, proxyURL: proxyURL, proxyNodeID: proxyNodeID}, nil
}

func (g *Gateway) releaseSelected(selected selectedKey) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inflight[selected.key.ID] > 0 {
		g.inflight[selected.key.ID]--
	}
	if selected.proxyNodeID != "" && g.proxyInflight[selected.proxyNodeID] > 0 {
		g.proxyInflight[selected.proxyNodeID]--
	}
}

func (g *Gateway) selectProxyNodeLocked(keyID string, mode string, nodes []store.ProxyNodeView) (store.ProxyNodeView, error) {
	candidates := make([]store.ProxyNodeView, 0, len(nodes))
	for _, node := range nodes {
		if !node.Enabled || !node.Online || strings.TrimSpace(node.ProxyURL) == "" {
			continue
		}
		maxConcurrency := node.MaxConcurrency
		if maxConcurrency <= 0 {
			maxConcurrency = 100
		}
		if g.proxyInflight[node.ID]+node.CurrentConnections >= maxConcurrency {
			continue
		}
		candidates = append(candidates, node)
	}
	if len(candidates) == 0 {
		return store.ProxyNodeView{}, errNoAvailableProxy
	}
	if strings.TrimSpace(mode) == store.ProxyModeKeyBinding {
		idx := int(stableHash(keyID) % uint32(len(candidates)))
		return candidates[idx], nil
	}
	idx := g.proxyRoundRobin % len(candidates)
	g.proxyRoundRobin++
	return candidates[idx], nil
}

func (g *Gateway) forward(ctx context.Context, selected selectedKey, method string, upstreamPath string, body []byte, isStream bool, w http.ResponseWriter, progress func(TestProgressEvent)) (int, http.Header, []byte, error) {
	if !isStream {
		status, header, respBody, err := g.doBufferedWithProgress(ctx, selected, method, upstreamPath, body, progress)
		if err != nil {
			http.Error(w, "upstream request failed", http.StatusBadGateway)
			return status, header, respBody, err
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
	start := time.Now()
	status, _, body, err := g.doBuffered(ctx, selected, http.MethodGet, modelsPath, nil)
	g.recordUpstreamResult(key.ID, status, body, err, time.Since(start).Milliseconds(), false)
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
	g.recordUpstreamResult(key.ID, status, respBody, forwardErr, capture.DurationMS, true)
	capture.StatusCode = status
	capture.ResponseHeader = redactHeader(header)
	capture.ResponseBody = string(respBody)
	if forwardErr != nil && !errors.Is(forwardErr, context.Canceled) {
		capture.Error = forwardErr.Error()
		captureProgress.emit(TestProgressEvent{Step: "upstream_error", Message: "上游请求失败", DurationMS: capture.DurationMS, Error: forwardErr.Error()})
	}
	capture.RequestHeader = map[string][]string{"Authorization": {"Bearer " + key.APIKey}}
	capture.NetworkEvents = captureProgress.events()
	if g.store != nil {
		emitProgress(progress, TestProgressEvent{Step: "record", Message: "写入测试结果和 Key 状态"})
		_ = g.store.SaveCapture(capture)
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
	if !shouldCaptureNetworkEvent(event) || strings.Contains(event.Error, context.Canceled.Error()) {
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

func proxyURLWithToken(proxyValue string, token string) string {
	proxyValue = strings.TrimSpace(proxyValue)
	token = strings.TrimSpace(token)
	if proxyValue == "" || token == "" {
		return proxyValue
	}
	u, err := url.Parse(proxyValue)
	if err != nil || u.Host == "" {
		return proxyValue
	}
	u.User = url.UserPassword("node", token)
	return u.String()
}

func stableHash(value string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(value))
	return h.Sum32()
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
	return false
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
