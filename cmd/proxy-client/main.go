package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const version = "dev"

type config struct {
	ServerURL      string
	Token          string
	ListenAddr     string
	NodeID         string
	NodeName       string
	PublicProxyURL string
	ReportInterval time.Duration
	MaxConcurrency int
}

type proxyServer struct {
	token  string
	client *http.Client

	active   atomic.Int64
	total    atomic.Int64
	failed   atomic.Int64
	bytesIn  atomic.Int64
	bytesOut atomic.Int64

	lastMu    sync.Mutex
	lastError string
}

type reportPayload struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	ProxyURL           string    `json:"proxyUrl,omitempty"`
	ListenAddr         string    `json:"listenAddr"`
	Version            string    `json:"version"`
	StartedAt          time.Time `json:"startedAt"`
	CurrentConnections int       `json:"currentConnections"`
	TotalRequests      int64     `json:"totalRequests"`
	FailedRequests     int64     `json:"failedRequests"`
	BytesIn            int64     `json:"bytesIn"`
	BytesOut           int64     `json:"bytesOut"`
	MaxConcurrency     int       `json:"maxConcurrency"`
	LastError          string    `json:"lastError,omitempty"`
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	ps := &proxyServer{
		token: cfg.Token,
		client: &http.Client{Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			DialContext:         (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
	startedAt := time.Now().UTC()
	go reportLoop(cfg, ps, startedAt)
	log.Printf("proxy client %s listening on %s as %s", version, cfg.ListenAddr, cfg.NodeID)
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: ps, ReadHeaderTimeout: 10 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

func loadConfig() (config, error) {
	serverURL := strings.TrimRight(strings.TrimSpace(os.Getenv("SERVER_URL")), "/")
	token := strings.TrimSpace(os.Getenv("PROXY_NODE_TOKEN"))
	if serverURL == "" {
		return config{}, fmt.Errorf("SERVER_URL is required")
	}
	if token == "" {
		return config{}, fmt.Errorf("PROXY_NODE_TOKEN is required")
	}
	if _, err := url.ParseRequestURI(serverURL); err != nil {
		return config{}, fmt.Errorf("SERVER_URL must be a valid URL: %w", err)
	}
	hostname, _ := os.Hostname()
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		hostname = "vps"
	}
	nodeID := strings.TrimSpace(os.Getenv("NODE_ID"))
	if nodeID == "" {
		nodeID = loadOrCreateNodeID(hostname)
	}
	nodeName := strings.TrimSpace(os.Getenv("NODE_NAME"))
	if nodeName == "" {
		nodeName = hostname
	}
	interval := 10 * time.Second
	if raw := strings.TrimSpace(os.Getenv("REPORT_INTERVAL")); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			interval = parsed
		}
	}
	maxConcurrency := 100
	if raw := strings.TrimSpace(os.Getenv("MAX_CONCURRENCY")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			maxConcurrency = parsed
		}
	}
	return config{
		ServerURL:      serverURL,
		Token:          token,
		ListenAddr:     getenv("LISTEN_ADDR", ":9070"),
		NodeID:         nodeID,
		NodeName:       nodeName,
		PublicProxyURL: strings.TrimSpace(os.Getenv("PUBLIC_PROXY_URL")),
		ReportInterval: interval,
		MaxConcurrency: maxConcurrency,
	}, nil
}

func loadOrCreateNodeID(hostname string) string {
	path := strings.TrimSpace(os.Getenv("NODE_ID_FILE"))
	if path == "" {
		if dir, err := os.UserConfigDir(); err == nil && dir != "" {
			path = filepath.Join(dir, "api-fucker", "proxy-node-id")
		} else {
			path = ".proxy-node-id"
		}
	}
	if raw, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(raw)); id != "" {
			return id
		}
	}
	id := safeID(hostname) + "-" + randomHex(6)
	_ = os.MkdirAll(filepath.Dir(path), 0o700)
	_ = os.WriteFile(path, []byte(id+"\n"), 0o600)
	return id
}

func (p *proxyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" && !r.URL.IsAbs() {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	}
	if !p.authorized(r) {
		w.Header().Set("Proxy-Authenticate", `Basic realm="api-fucker-proxy"`)
		http.Error(w, "proxy authentication required", http.StatusProxyAuthRequired)
		return
	}
	p.total.Add(1)
	p.active.Add(1)
	defer p.active.Add(-1)

	if r.Method == http.MethodConnect {
		if err := p.handleConnect(w, r); err != nil {
			p.recordFailure(err)
		}
		return
	}
	if err := p.handleHTTP(w, r); err != nil {
		p.recordFailure(err)
		http.Error(w, "proxy request failed", http.StatusBadGateway)
	}
}

func (p *proxyServer) authorized(r *http.Request) bool {
	if token := bearerToken(r.Header.Get("Proxy-Authorization")); token != "" {
		return sameToken(token, p.token)
	}
	if token := bearerToken(r.Header.Get("Authorization")); token != "" {
		return sameToken(token, p.token)
	}
	_, password, ok := parseBasicAuth(r.Header.Get("Proxy-Authorization"))
	return ok && sameToken(password, p.token)
}

func (p *proxyServer) handleConnect(w http.ResponseWriter, r *http.Request) error {
	targetConn, err := net.DialTimeout("tcp", r.Host, 30*time.Second)
	if err != nil {
		http.Error(w, "connect target failed", http.StatusBadGateway)
		return err
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = targetConn.Close()
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return errors.New("hijacking not supported")
	}
	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		_ = targetConn.Close()
		return err
	}
	_, _ = clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	p.tunnel(clientConn, targetConn)
	return nil
}

func (p *proxyServer) tunnel(clientConn, targetConn net.Conn) {
	defer clientConn.Close()
	defer targetConn.Close()
	done := make(chan struct{}, 2)
	go func() {
		n, _ := io.Copy(targetConn, clientConn)
		p.bytesIn.Add(n)
		done <- struct{}{}
	}()
	go func() {
		n, _ := io.Copy(clientConn, targetConn)
		p.bytesOut.Add(n)
		done <- struct{}{}
	}()
	<-done
}

func (p *proxyServer) handleHTTP(w http.ResponseWriter, r *http.Request) error {
	if !r.URL.IsAbs() {
		return fmt.Errorf("proxy requests must use absolute URLs")
	}
	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.Header = r.Header.Clone()
	out.Header.Del("Proxy-Authorization")
	out.Header.Del("Proxy-Authenticate")
	resp, err := p.client.Do(out)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	n, err := io.Copy(w, resp.Body)
	p.bytesOut.Add(n)
	if r.ContentLength > 0 {
		p.bytesIn.Add(r.ContentLength)
	}
	return err
}

func reportLoop(cfg config, ps *proxyServer, startedAt time.Time) {
	ticker := time.NewTicker(cfg.ReportInterval)
	defer ticker.Stop()
	for {
		reportOnce(cfg, ps, startedAt)
		<-ticker.C
	}
}

func reportOnce(cfg config, ps *proxyServer, startedAt time.Time) {
	payload := reportPayload{
		ID:                 cfg.NodeID,
		Name:               cfg.NodeName,
		ProxyURL:           cfg.PublicProxyURL,
		ListenAddr:         cfg.ListenAddr,
		Version:            version,
		StartedAt:          startedAt,
		CurrentConnections: int(ps.active.Load()),
		TotalRequests:      ps.total.Load(),
		FailedRequests:     ps.failed.Load(),
		BytesIn:            ps.bytesIn.Load(),
		BytesOut:           ps.bytesOut.Load(),
		MaxConcurrency:     cfg.MaxConcurrency,
		LastError:          ps.lastErrorValue(),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		ps.setLastError(err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.ServerURL+"/api/admin/proxies/report", strings.NewReader(string(body)))
	if err != nil {
		ps.setLastError(err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		ps.setLastError(err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		ps.setLastError(fmt.Errorf("report returned %s", resp.Status))
		return
	}
	ps.setLastError(nil)
}

func (p *proxyServer) recordFailure(err error) {
	p.failed.Add(1)
	p.setLastError(err)
}

func (p *proxyServer) setLastError(err error) {
	p.lastMu.Lock()
	defer p.lastMu.Unlock()
	if err == nil {
		p.lastError = ""
		return
	}
	p.lastError = err.Error()
}

func (p *proxyServer) lastErrorValue() string {
	p.lastMu.Lock()
	defer p.lastMu.Unlock()
	return p.lastError
}

func parseBasicAuth(value string) (string, string, bool) {
	req := &http.Request{Header: http.Header{"Authorization": []string{value}}}
	return req.BasicAuth()
}

func bearerToken(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(value), "bearer ") {
		return strings.TrimSpace(value[7:])
	}
	return ""
}

func sameToken(got, want string) bool {
	got = strings.TrimSpace(got)
	want = strings.TrimSpace(want)
	return got != "" && want != "" && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func copyHeader(dst, src http.Header) {
	for k, values := range src {
		for _, value := range values {
			dst.Add(k, value)
		}
	}
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func safeID(value string) string {
	value = strings.ToLower(value)
	var b strings.Builder
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "vps"
	}
	return b.String()
}

func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
