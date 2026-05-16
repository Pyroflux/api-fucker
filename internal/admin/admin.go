package admin

import (
	"context"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"api-fucker/internal/gateway"
	"api-fucker/internal/store"
)

//go:embed static/*
var staticFS embed.FS

type Handler struct {
	store              *store.Store
	adminToken         string
	currentConcurrency func() map[string]int
	upstream           upstreamTester
}

type loginRequest struct {
	Token string `json:"token"`
}

type upstreamTester interface {
	ListModelsForKey(ctx context.Context, key store.Key, cfg store.GlobalConfig) (gateway.ModelListResult, error)
	TestKey(ctx context.Context, key store.Key, cfg store.GlobalConfig, model string, prompt string) (gateway.UpstreamTestResult, error)
}

type upstreamProgressTester interface {
	TestKeyWithProgress(ctx context.Context, key store.Key, cfg store.GlobalConfig, model string, prompt string, progress func(gateway.TestProgressEvent)) (gateway.UpstreamTestResult, error)
}

type testKeyRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type importKeysRequest struct {
	Text           string `json:"text"`
	BaseURL        string `json:"baseUrl"`
	ProxyURL       string `json:"proxyUrl"`
	MaxConcurrency int    `json:"maxConcurrency"`
	Enabled        bool   `json:"enabled"`
}

type keyListResponse struct {
	Items      []store.KeyView `json:"items"`
	Total      int             `json:"total"`
	Page       int             `json:"page"`
	PageSize   int             `json:"pageSize"`
	TotalPages int             `json:"totalPages"`
	Stats      keyListStats    `json:"stats"`
}

type keyListStats struct {
	Total    int `json:"total"`
	Healthy  int `json:"healthy"`
	Paused   int `json:"paused"`
	Inflight int `json:"inflight"`
}

func New(st *store.Store, token string, currentConcurrency func() map[string]int, upstream ...upstreamTester) *Handler {
	if currentConcurrency == nil {
		currentConcurrency = func() map[string]int { return map[string]int{} }
	}
	var tester upstreamTester
	if len(upstream) > 0 {
		tester = upstream[0]
	}
	return &Handler{store: st, adminToken: normalizeAdminToken(token), currentConcurrency: currentConcurrency, upstream: tester}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/admin" {
		h.serveAdmin(w, r)
		return
	}

	if r.URL.Path == "/api/admin/login" && r.Method == http.MethodPost {
		h.login(w, r)
		return
	}
	if r.URL.Path == "/api/admin/logout" && r.Method == http.MethodPost {
		h.logout(w, r)
		return
	}

	if !h.authorized(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	switch {
	case r.URL.Path == "/api/admin/config":
		h.config(w, r)
	case r.URL.Path == "/api/admin/keys":
		h.keys(w, r)
	case r.URL.Path == "/api/admin/keys/import":
		h.importKeys(w, r)
	case r.URL.Path == "/api/admin/keys/restore-all":
		h.restoreAllKeys(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/admin/keys/"):
		h.keyByID(w, r)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	}
}

func (h *Handler) restoreAllKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	restored, err := h.store.RestoreAllKeys()
	respond(w, map[string]int{"restored": restored}, err)
}

func (h *Handler) importKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	var req importKeysRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	inputs := parseImportText(req)
	result, err := h.store.ImportKeys(inputs)
	respond(w, result, err)
}

func parseImportText(req importKeysRequest) []store.ImportKeyInput {
	lines := strings.Split(req.Text, "\n")
	inputs := make([]store.ImportKeyInput, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := splitImportLine(line)
		input := store.ImportKeyInput{
			MaxConcurrency: req.MaxConcurrency,
			Enabled:        req.Enabled,
			BaseURL:        strings.TrimSpace(req.BaseURL),
			ProxyURL:       strings.TrimSpace(req.ProxyURL),
		}
		switch len(parts) {
		case 1:
			input.APIKey = parts[0]
		case 2:
			input.Name = parts[0]
			input.APIKey = parts[1]
		default:
			input.Name = parts[0]
			input.APIKey = parts[1]
			input.BaseURL = parts[2]
			if len(parts) > 3 {
				input.ProxyURL = parts[3]
			}
		}
		inputs = append(inputs, input)
	}
	return inputs
}

func splitImportLine(line string) []string {
	separator := ","
	if strings.Contains(line, "\t") {
		separator = "\t"
	}
	raw := strings.Split(line, separator)
	parts := make([]string, 0, len(raw))
	for _, part := range raw {
		parts = append(parts, strings.TrimSpace(part))
	}
	return parts
}

func (h *Handler) serveAdmin(w http.ResponseWriter, _ *http.Request) {
	raw, err := staticFS.ReadFile("static/admin.html")
	if err != nil {
		http.Error(w, "admin page missing", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(raw)
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	if h.adminToken == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "ADMIN_TOKEN is not configured"})
		return
	}
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	if !sameAdminToken(req.Token, h.adminToken) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid token"})
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     "admin_token",
		Value:    sessionCookieValue(h.adminToken),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Now().Add(24 * time.Hour),
	})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *Handler) logout(w http.ResponseWriter, _ *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     "admin_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0),
	})
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *Handler) authorized(r *http.Request) bool {
	if h.adminToken == "" {
		return false
	}
	cookie, err := r.Cookie("admin_token")
	if err == nil && sameSessionCookie(cookie.Value, h.adminToken) {
		return true
	}
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return sameAdminToken(strings.TrimPrefix(auth, "Bearer "), h.adminToken)
	}
	return false
}

func normalizeAdminToken(token string) string {
	return strings.TrimSpace(token)
}

func sameAdminToken(got, want string) bool {
	got = normalizeAdminToken(got)
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func sessionCookieValue(token string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(token))
}

func sameSessionCookie(value, token string) bool {
	if decoded, err := base64.RawURLEncoding.DecodeString(value); err == nil && sameAdminToken(string(decoded), token) {
		return true
	}
	// Keep compatibility with cookies issued before the value was encoded.
	return sameAdminToken(value, token)
}

func (h *Handler) config(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		cfg, err := h.store.GlobalConfig()
		respond(w, cfg, err)
	case http.MethodPut:
		var cfg store.GlobalConfig
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
			return
		}
		respond(w, map[string]bool{"ok": true}, h.store.SaveGlobalConfig(cfg))
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func (h *Handler) keys(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		keys, err := h.store.ListKeys()
		if err != nil {
			respond(w, nil, err)
			return
		}
		inflight := h.currentConcurrency()
		views := make([]store.KeyView, 0, len(keys))
		stats := keyListStats{Total: len(keys)}
		for _, key := range keys {
			current := inflight[key.ID]
			if key.Enabled && !key.Paused && !key.BalanceDepleted {
				stats.Healthy++
			}
			if key.Paused || key.BalanceDepleted {
				stats.Paused++
			}
			stats.Inflight += current
			view := store.KeyView{Key: key, CurrentConcurrency: current}
			if matchesKeyStatusFilter(view, r.URL.Query().Get("status")) {
				views = append(views, view)
			}
		}
		sortKeyViews(views, r.URL.Query().Get("sort"), r.URL.Query().Get("order"))
		page, pageSize := pagination(r)
		totalPages := 0
		if len(views) > 0 {
			totalPages = (len(views) + pageSize - 1) / pageSize
		}
		if totalPages > 0 && page > totalPages {
			page = totalPages
		}
		start := (page - 1) * pageSize
		if start > len(views) {
			start = len(views)
		}
		end := start + pageSize
		if end > len(views) {
			end = len(views)
		}
		writeJSON(w, http.StatusOK, keyListResponse{
			Items:      views[start:end],
			Total:      len(views),
			Page:       page,
			PageSize:   pageSize,
			TotalPages: totalPages,
			Stats:      stats,
		})
	case http.MethodPost:
		var key store.Key
		if err := json.NewDecoder(r.Body).Decode(&key); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
			return
		}
		created, err := h.store.CreateKey(key)
		respond(w, created, err)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func pagination(r *http.Request) (int, int) {
	page := intQuery(r, "page", 1)
	pageSize := intQuery(r, "pageSize", 100)
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 100
	}
	if pageSize > 1000 {
		pageSize = 1000
	}
	return page, pageSize
}

func intQuery(r *http.Request, name string, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func (h *Handler) keyByID(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/admin/keys/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch {
	case action == "" && r.Method == http.MethodGet:
		key, err := h.store.GetKey(id)
		respond(w, key, err)
	case action == "" && r.Method == http.MethodPut:
		var key store.Key
		if err := json.NewDecoder(r.Body).Decode(&key); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
			return
		}
		updated, err := h.store.UpdateKey(id, key)
		respond(w, updated, err)
	case action == "" && r.Method == http.MethodDelete:
		respond(w, map[string]bool{"ok": true}, h.store.DeleteKey(id))
	case action == "restore" && r.Method == http.MethodPost:
		key, err := h.store.RestoreKey(id)
		respond(w, key, err)
	case action == "capture" && r.Method == http.MethodGet:
		capture, err := h.store.GetCapture(id)
		respond(w, capture, err)
	case action == "models" && r.Method == http.MethodGet:
		h.keyModels(w, r, id)
	case action == "test" && r.Method == http.MethodPost:
		h.testKey(w, r, id)
	case action == "test-progress" && r.Method == http.MethodPost:
		h.testKeyProgress(w, r, id)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

func matchesKeyStatusFilter(k store.KeyView, status string) bool {
	switch strings.TrimSpace(status) {
	case "", "all":
		return true
	case "routable":
		return k.Enabled && !k.Paused && !k.BalanceDepleted
	case "unavailable":
		return !k.Enabled || k.Paused || k.BalanceDepleted
	case "error":
		return k.ConsecutiveErrors > 0 || strings.TrimSpace(k.LastError) != ""
	default:
		return true
	}
}

func sortKeyViews(keys []store.KeyView, sortBy string, order string) {
	if strings.TrimSpace(sortBy) != "lastUsedAt" {
		return
	}
	asc := strings.EqualFold(strings.TrimSpace(order), "asc")
	sort.SliceStable(keys, func(i, j int) bool {
		a := keys[i].LastUsedAt
		b := keys[j].LastUsedAt
		if a.IsZero() != b.IsZero() {
			if asc {
				return a.IsZero()
			}
			return !a.IsZero()
		}
		if asc {
			return a.Before(b)
		}
		return a.After(b)
	})
}

func (h *Handler) keyModels(w http.ResponseWriter, r *http.Request, id string) {
	if h.upstream == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "upstream tester is not configured"})
		return
	}
	key, cfg, err := h.keyAndConfig(id)
	if err != nil {
		respond(w, nil, err)
		return
	}
	result, err := h.upstream.ListModelsForKey(r.Context(), key, cfg)
	respond(w, result, err)
}

func (h *Handler) testKey(w http.ResponseWriter, r *http.Request, id string) {
	if h.upstream == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "upstream tester is not configured"})
		return
	}
	var req testKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	key, cfg, err := h.keyAndConfig(id)
	if err != nil {
		respond(w, nil, err)
		return
	}
	result, err := h.upstream.TestKey(r.Context(), key, cfg, req.Model, req.Prompt)
	if err != nil {
		if result.Error == "" {
			result.Error = err.Error()
		}
		writeJSON(w, http.StatusBadGateway, result)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) testKeyProgress(w http.ResponseWriter, r *http.Request, id string) {
	tester, ok := h.upstream.(upstreamProgressTester)
	if h.upstream == nil || !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "upstream progress tester is not configured"})
		return
	}
	var req testKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}
	key, cfg, err := h.keyAndConfig(id)
	if err != nil {
		respond(w, nil, err)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	events := make(chan gateway.TestProgressEvent, 32)
	go func() {
		defer close(events)
		result, err := tester.TestKeyWithProgress(r.Context(), key, cfg, req.Model, req.Prompt, func(event gateway.TestProgressEvent) {
			select {
			case events <- event:
			case <-r.Context().Done():
			}
		})
		if err != nil {
			if result.Error == "" {
				result.Error = err.Error()
			}
			events <- gateway.TestProgressEvent{Step: "error", Message: "测试失败", Error: err.Error(), Result: &result}
			return
		}
		events <- gateway.TestProgressEvent{Step: "done", Message: "测试完成", Result: &result}
	}()

	enc := json.NewEncoder(w)
	for event := range events {
		if err := enc.Encode(event); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func (h *Handler) keyAndConfig(id string) (store.Key, store.GlobalConfig, error) {
	key, err := h.store.GetKey(id)
	if err != nil {
		return store.Key{}, store.GlobalConfig{}, err
	}
	cfg, err := h.store.GlobalConfig()
	if err != nil {
		return store.Key{}, store.GlobalConfig{}, err
	}
	return key, cfg, nil
}

func respond(w http.ResponseWriter, value any, err error) {
	if err == nil {
		writeJSON(w, http.StatusOK, value)
		return
	}
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
