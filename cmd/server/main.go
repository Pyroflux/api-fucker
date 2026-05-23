package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"api-fucker/internal/admin"
	"api-fucker/internal/gateway"
	"api-fucker/internal/store"
)

func main() {
	dataPath := getenv("DATA_PATH", "./data.db")
	port := getenv("PORT", "8080")
	adminPort := getenv("ADMIN_PORT", "8081")
	if port == adminPort {
		log.Fatalf("PORT and ADMIN_PORT must be different, got %s", port)
	}

	st, err := store.Open(dataPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	proxyNodeToken := os.Getenv("PROXY_NODE_TOKEN")
	gw := gateway.New(st, &http.Client{Timeout: 0}, proxyNodeToken)
	adminHandler := admin.New(st, os.Getenv("ADMIN_TOKEN"), gw.CurrentConcurrency, gw)
	adminHandler.SetProxyNodeToken(proxyNodeToken)
	adminHandler.SetQueueDepth(gw.QueueDepth)
	adminHandler.SetKeyMinuteUsage(gw.KeyMinuteUsage)

	apiMux := http.NewServeMux()
	apiMux.Handle("/v1/chat/completions", gw)
	apiMux.Handle("/v1/models", gw)
	apiMux.HandleFunc("/healthz", healthz)

	adminMux := http.NewServeMux()
	adminMux.Handle("/admin", adminHandler)
	adminMux.Handle("/api/admin/", adminHandler)
	adminMux.HandleFunc("/healthz", healthz)

	apiSrv := &http.Server{
		Addr:              ":" + port,
		Handler:           logging("api", ipAccessControl(st, apiMux)),
		ReadHeaderTimeout: 10 * time.Second,
	}
	adminSrv := &http.Server{
		Addr:              ":" + adminPort,
		Handler:           logging("admin", adminMux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 2)
	go serve("request api", apiSrv, errCh)
	go serve("admin", adminSrv, errCh)
	log.Fatal(<-errCh)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func serve(name string, srv *http.Server, errCh chan<- error) {
	log.Printf("%s listening on %s", name, srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		errCh <- fmt.Errorf("%s server failed: %w", name, err)
	}
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func logging(name string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("[%s] %s %s %s", name, r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func ipAccessControl(st *store.Store, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/") {
			next.ServeHTTP(w, r)
			return
		}
		cfg, err := st.GlobalConfig()
		if err != nil {
			http.Error(w, "load ip access config failed", http.StatusInternalServerError)
			return
		}
		clientIP := clientIPFromRemoteAddr(r.RemoteAddr)
		if !ipAllowed(clientIP, cfg.IPAllowlist, cfg.IPBlocklist) {
			http.Error(w, fmt.Sprintf("your ip %s is not allowed", clientIP), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func clientIPFromRemoteAddr(remoteAddr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(strings.TrimSpace(remoteAddr), "[]")
}

func ipAllowed(ip string, allowlist []string, blocklist []string) bool {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return false
	}
	if matchesIPRules(ip, blocklist) {
		return false
	}
	if len(normalizeIPRules(allowlist)) == 0 {
		return true
	}
	return matchesIPRules(ip, allowlist)
}

func matchesIPRules(ip string, rules []string) bool {
	for _, rule := range normalizeIPRules(rules) {
		if strings.Contains(rule, "*") {
			if strings.HasPrefix(ip, strings.Split(rule, "*")[0]) {
				return true
			}
			continue
		}
		if ip == rule {
			return true
		}
	}
	return false
}

func normalizeIPRules(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range strings.FieldsFunc(value, func(r rune) bool {
			return r == ',' || r == '\n' || r == '\r' || r == '\t'
		}) {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}
