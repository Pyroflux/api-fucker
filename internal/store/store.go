package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	keysBucket          = "keys"
	metaBucket          = "meta"
	captureBucket       = "captures"
	captureStatusBucket = "capture_status"
	metricsBucket       = "metrics"
	proxyNodesBucket    = "proxy_nodes"
	globalConfigID      = "global"
	metricsMaxAge       = 7 * 24 * time.Hour

	ProxyModeRoundRobin  = "round_robin"
	ProxyModeKeyBinding  = "key_binding"
	ProxyNodeOnlineAfter = 30 * time.Second

	defaultKeyAutoPauseThreshold = 3
)

var (
	ErrNotFound = errors.New("not found")
)

type Store struct {
	db *bolt.DB
	*runtimeState
}

type GlobalConfig struct {
	UpstreamBaseURL         string    `json:"upstreamBaseUrl"`
	DefaultProxyURL         string    `json:"defaultProxyUrl"`
	IPAllowlist             []string  `json:"ipAllowlist,omitempty"`
	IPBlocklist             []string  `json:"ipBlocklist,omitempty"`
	ProxyPoolEnabled        bool      `json:"proxyPoolEnabled"`
	ProxyMode               string    `json:"proxyMode"`
	GlobalMaxConcurrency    int       `json:"globalMaxConcurrency"`
	MaxQueueSize            int       `json:"maxQueueSize"`
	QueueTimeoutMS          int       `json:"queueTimeoutMs"`
	KeyMaxRequestsPerMinute int       `json:"keyMaxRequestsPerMinute"`
	LaunchIntervalMS        int       `json:"launchIntervalMs"`
	Key429Threshold         int       `json:"key429Threshold"`
	Key429CooldownMinutes   int       `json:"key429CooldownMinutes"`
	KeyAutoPauseEnabled     bool      `json:"keyAutoPauseEnabled"`
	KeyAutoPauseThreshold   int       `json:"keyAutoPauseThreshold"`
	UpdatedAt               time.Time `json:"updatedAt"`
}

type Key struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	APIKey            string    `json:"apiKey"`
	BaseURL           string    `json:"baseUrl"`
	ProxyURL          string    `json:"proxyUrl"`
	MaxConcurrency    int       `json:"maxConcurrency"`
	Enabled           bool      `json:"enabled"`
	Paused            bool      `json:"paused"`
	Consecutive429    int       `json:"consecutive429"`
	RateLimitedUntil  time.Time `json:"rateLimitedUntil"`
	ManualPaused      bool      `json:"manualPaused"`
	BalanceDepleted   bool      `json:"balanceDepleted"`
	ConsecutiveErrors int       `json:"consecutiveErrors"`
	LastError         string    `json:"lastError"`
	LastStatusCode    int       `json:"lastStatusCode"`
	LastFirstByteMS   *int64    `json:"lastFirstByteMs,omitempty"`
	LastResponseMS    *int64    `json:"lastResponseMs,omitempty"`
	LastUsedAt        time.Time `json:"lastUsedAt"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

func (k Key) IsRateLimited(now time.Time) bool { return now.Before(k.RateLimitedUntil) }

type KeyView struct {
	Key
	RateLimited           bool `json:"rateLimited"`
	CurrentConcurrency    int  `json:"currentConcurrency"`
	CurrentMinuteRequests int  `json:"currentMinuteRequests"`
}

type ImportKeyInput struct {
	Name           string `json:"name"`
	APIKey         string `json:"apiKey"`
	BaseURL        string `json:"baseUrl"`
	ProxyURL       string `json:"proxyUrl"`
	MaxConcurrency int    `json:"maxConcurrency"`
	Enabled        bool   `json:"enabled"`
}

type ImportResult struct {
	Imported int      `json:"imported"`
	Skipped  int      `json:"skipped"`
	Errors   []string `json:"errors"`
}

type BulkUpdateResult struct {
	Updated int `json:"updated"`
}

type BulkDeleteResult struct {
	Deleted int `json:"deleted"`
}

type ProxyNode struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	ProxyURL           string    `json:"proxyUrl"`
	Enabled            bool      `json:"enabled"`
	MaxConcurrency     int       `json:"maxConcurrency"`
	LastReportAt       time.Time `json:"lastReportAt"`
	LastRemoteAddr     string    `json:"lastRemoteAddr"`
	Version            string    `json:"version"`
	StartedAt          time.Time `json:"startedAt"`
	ListenAddr         string    `json:"listenAddr"`
	CurrentConnections int       `json:"currentConnections"`
	TotalRequests      int64     `json:"totalRequests"`
	FailedRequests     int64     `json:"failedRequests"`
	BytesIn            int64     `json:"bytesIn"`
	BytesOut           int64     `json:"bytesOut"`
	LastError          string    `json:"lastError"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

type ProxyNodeView struct {
	ProxyNode
	Online bool   `json:"online"`
	Status string `json:"status"`
}

type ProxyNodeReport struct {
	ID                 string    `json:"id"`
	Name               string    `json:"name"`
	ProxyURL           string    `json:"proxyUrl"`
	ListenAddr         string    `json:"listenAddr"`
	Version            string    `json:"version"`
	StartedAt          time.Time `json:"startedAt"`
	CurrentConnections int       `json:"currentConnections"`
	TotalRequests      int64     `json:"totalRequests"`
	FailedRequests     int64     `json:"failedRequests"`
	BytesIn            int64     `json:"bytesIn"`
	BytesOut           int64     `json:"bytesOut"`
	MaxConcurrency     int       `json:"maxConcurrency"`
	LastError          string    `json:"lastError"`
}

type MetricSample struct {
	Time        time.Time
	FirstByteMS *int64
	Concurrency int
	QueueDepth  int
	Error       bool
}

type metricBucketRecord struct {
	Time             time.Time `json:"time"`
	Requests         int       `json:"requests"`
	FirstByteTotalMS int64     `json:"firstByteTotalMs"`
	FirstByteSamples int       `json:"firstByteSamples"`
	MaxConcurrency   int       `json:"maxConcurrency"`
	MaxQueueDepth    int       `json:"maxQueueDepth"`
	Errors           int       `json:"errors"`
}

type MetricPoint struct {
	Time        time.Time `json:"time"`
	FirstByteMS *int64    `json:"firstByteMs,omitempty"`
	Concurrency int       `json:"concurrency"`
	QueueDepth  int       `json:"queueDepth"`
	Errors      int       `json:"errors"`
	Requests    int       `json:"requests"`
}

type Capture struct {
	revision       uint64
	generation     uint64
	KeyID          string                `json:"keyId"`
	Time           time.Time             `json:"time"`
	DurationMS     int64                 `json:"durationMs"`
	RequestMethod  string                `json:"requestMethod"`
	RequestURL     string                `json:"requestUrl"`
	RequestHeader  map[string][]string   `json:"requestHeader"`
	RequestBody    string                `json:"requestBody"`
	StatusCode     int                   `json:"statusCode"`
	ResponseHeader map[string][]string   `json:"responseHeader"`
	ResponseBody   string                `json:"responseBody"`
	Error          string                `json:"error"`
	NetworkEvents  []CaptureNetworkEvent `json:"networkEvents,omitempty"`
	Truncated      bool                  `json:"truncated,omitempty"`
}

type CaptureNetworkEvent struct {
	Step       string `json:"step"`
	Message    string `json:"message"`
	Attempt    int    `json:"attempt,omitempty"`
	DurationMS int64  `json:"durationMs,omitempty"`
	ElapsedMS  int64  `json:"elapsedMs"`
	Error      string `json:"error,omitempty"`
}

func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	st := &Store{db: db}
	if err := st.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := st.loadRuntime(); err != nil {
		_ = db.Close()
		return nil, err
	}
	go st.persistenceLoop()
	return st, nil
}

func (s *Store) Close() error {
	return s.closeRuntime()
}

func (s *Store) init() error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(keysBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(metaBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(captureBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(captureStatusBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(metricsBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(proxyNodesBucket)); err != nil {
			return err
		}
		b := tx.Bucket([]byte(metaBucket))
		if b.Get([]byte(globalConfigID)) == nil {
			cfg := defaultGlobalConfig()
			raw, err := json.Marshal(cfg)
			if err != nil {
				return err
			}
			return b.Put([]byte(globalConfigID), raw)
		}
		return nil
	})
}

func (s *Store) GlobalConfig() (GlobalConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneConfig(s.config), nil
}

func (s *Store) SaveGlobalConfig(cfg GlobalConfig) error {
	if cfg.Key429Threshold < 0 || cfg.Key429CooldownMinutes < 0 || int64(cfg.Key429CooldownMinutes) > int64((1<<63-1)/time.Minute) {
		return fmt.Errorf("429 threshold and cooldown must be positive and cooldown must not overflow")
	}
	if cfg.KeyAutoPauseThreshold < 0 {
		return fmt.Errorf("key auto-pause threshold cannot be negative")
	}
	cfg = normalizeGlobalConfig(cfg)
	if err := validateRequiredURL(cfg.UpstreamBaseURL, "upstream base URL"); err != nil {
		return err
	}
	if err := validateOptionalURL(cfg.DefaultProxyURL, "default proxy URL"); err != nil {
		return err
	}
	if cfg.GlobalMaxConcurrency < 0 {
		return fmt.Errorf("global max concurrency cannot be negative")
	}
	if cfg.MaxQueueSize < 0 {
		return fmt.Errorf("max queue size cannot be negative")
	}
	if cfg.QueueTimeoutMS < 0 {
		return fmt.Errorf("queue timeout cannot be negative")
	}
	if cfg.KeyMaxRequestsPerMinute < 0 {
		return fmt.Errorf("key max requests per minute cannot be negative")
	}
	if cfg.LaunchIntervalMS < 0 {
		return fmt.Errorf("launch interval cannot be negative")
	}
	cfg.ProxyMode = normalizeProxyMode(cfg.ProxyMode)
	cfg.IPAllowlist = normalizeRules(cfg.IPAllowlist)
	cfg.IPBlocklist = normalizeRules(cfg.IPBlocklist)
	cfg.UpdatedAt = time.Now().UTC()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.config = cloneConfig(cfg)
	s.dirtyConfig = s.nextRevisionLocked()
	s.mu.Unlock()
	return s.Flush()
}

func normalizeProxyMode(mode string) string {
	switch strings.TrimSpace(mode) {
	case ProxyModeKeyBinding:
		return ProxyModeKeyBinding
	default:
		return ProxyModeRoundRobin
	}
}

func defaultGlobalConfig() GlobalConfig {
	return GlobalConfig{
		Key429Threshold:       1,
		Key429CooldownMinutes: 10,
		KeyAutoPauseEnabled:   true,
		KeyAutoPauseThreshold: defaultKeyAutoPauseThreshold,
		UpdatedAt:             time.Now().UTC(),
	}
}

func normalizeGlobalConfig(cfg GlobalConfig) GlobalConfig {
	if cfg.Key429Threshold == 0 {
		cfg.Key429Threshold = 1
	}
	if cfg.Key429CooldownMinutes == 0 {
		cfg.Key429CooldownMinutes = 10
	}
	if cfg.KeyAutoPauseThreshold == 0 {
		cfg.KeyAutoPauseEnabled = true
		cfg.KeyAutoPauseThreshold = defaultKeyAutoPauseThreshold
	}
	return cfg
}

func normalizeRules(values []string) []string {
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

func (s *Store) ListKeys() ([]Key, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]Key, 0, len(s.keys))
	for _, id := range s.keyOrder {
		keys = append(keys, cloneKey(s.keys[id]))
	}
	return keys, nil
}

func (s *Store) GetKey(id string) (Key, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.keys[id]
	if !ok {
		return Key{}, ErrNotFound
	}
	return cloneKey(key), nil
}

func (s *Store) CreateKey(key Key) (Key, error) {
	if key.ID == "" {
		key.ID = randomID()
	}
	now := time.Now().UTC()
	key.CreatedAt = now
	key.UpdatedAt = now
	if key.MaxConcurrency <= 0 {
		key.MaxConcurrency = 1
	}
	key.Enabled = true
	if err := ValidateKey(key); err != nil {
		return Key{}, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return Key{}, ErrClosed
	}
	if _, exists := s.keys[key.ID]; exists {
		s.mu.Unlock()
		return Key{}, fmt.Errorf("key id already exists")
	}
	s.keys[key.ID] = cloneKey(key)
	s.dirtyKeys[key.ID] = s.nextRevisionLocked()
	s.keyGeneration[key.ID] = s.revision
	s.ignoreStoredCapture[key.ID] = true
	s.dirtyCaptureDeletes[key.ID] = s.revision
	order := make([]string, len(s.keyOrder)+1)
	idx := sort.SearchStrings(s.keyOrder, key.ID)
	copy(order, s.keyOrder[:idx])
	order[idx] = key.ID
	copy(order[idx+1:], s.keyOrder[idx:])
	s.keyOrder = order
	s.mu.Unlock()
	return key, s.Flush()
}

func (s *Store) UpdateKey(id string, patch Key) (Key, error) {
	if patch.MaxConcurrency <= 0 {
		patch.MaxConcurrency = 1
	}
	if err := ValidateKey(patch); err != nil {
		return Key{}, err
	}
	key, err := s.mutateKey(id, func(key *Key) {
		key.Name = patch.Name
		key.APIKey = patch.APIKey
		key.BaseURL = patch.BaseURL
		key.ProxyURL = patch.ProxyURL
		key.MaxConcurrency = patch.MaxConcurrency
		key.Enabled = patch.Enabled
		key.Paused = patch.Paused
		key.BalanceDepleted = patch.BalanceDepleted
	})
	if err != nil {
		return key, err
	}
	return key, s.Flush()
}

func (s *Store) UpdateKeysMaxConcurrency(ids []string, maxConcurrency int) (BulkUpdateResult, error) {
	if maxConcurrency <= 0 {
		return BulkUpdateResult{}, fmt.Errorf("max concurrency must be greater than zero")
	}
	count, err := s.changeKeys(ids, func(key *Key) bool { key.MaxConcurrency = maxConcurrency; return true })
	return BulkUpdateResult{Updated: count}, err
}

func (s *Store) PauseKeys(ids []string) (BulkUpdateResult, error) {
	count, err := s.changeKeys(ids, func(key *Key) bool {
		if key.ManualPaused {
			return false
		}
		key.ManualPaused = true
		return true
	})
	return BulkUpdateResult{Updated: count}, err
}

func (s *Store) DeleteKeys(ids []string) (BulkDeleteResult, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return BulkDeleteResult{}, ErrClosed
	}
	wanted := idSet(ids)
	all := len(wanted) == 0
	result := BulkDeleteResult{}
	order := make([]string, 0, len(s.keyOrder))
	for _, id := range s.keyOrder {
		if !all && !wanted[id] {
			order = append(order, id)
			continue
		}
		delete(s.keys, id)
		delete(s.keyGeneration, id)
		delete(s.latestCaptureSequence, id)
		delete(s.ignoreStoredCapture, id)
		s.dirtyKeys[id] = s.nextRevisionLocked()
		s.dirtyCaptureDeletes[id] = s.revision
		s.dropPendingCaptureLocked(id, false)
		result.Deleted++
	}
	s.keyOrder = order
	s.mu.Unlock()
	return result, s.Flush()
}

func (s *Store) ImportKeys(inputs []ImportKeyInput) (ImportResult, error) {
	result := ImportResult{Errors: make([]string, 0)}
	existing, err := s.ListKeys()
	if err != nil {
		return result, err
	}
	seen := make(map[string]bool, len(existing)+len(inputs))
	for _, key := range existing {
		seen[strings.TrimSpace(key.APIKey)] = true
	}

	for i, input := range inputs {
		apiKey := strings.TrimSpace(input.APIKey)
		if apiKey == "" {
			result.Skipped++
			result.Errors = append(result.Errors, fmt.Sprintf("line %d: api key is empty", i+1))
			continue
		}
		if seen[apiKey] {
			result.Skipped++
			continue
		}
		name := strings.TrimSpace(input.Name)
		if name == "" {
			name = fmt.Sprintf("key-%d", time.Now().UnixNano())
		}
		maxConcurrency := input.MaxConcurrency
		if maxConcurrency <= 0 {
			maxConcurrency = 1
		}
		enabled := input.Enabled
		key := Key{
			Name:           name,
			APIKey:         apiKey,
			BaseURL:        strings.TrimSpace(input.BaseURL),
			ProxyURL:       strings.TrimSpace(input.ProxyURL),
			MaxConcurrency: maxConcurrency,
			Enabled:        enabled,
		}
		if _, err := s.CreateKey(key); err != nil {
			result.Skipped++
			result.Errors = append(result.Errors, fmt.Sprintf("line %d: %v", i+1, err))
			continue
		}
		seen[apiKey] = true
		result.Imported++
	}
	return result, nil
}

func (s *Store) DeleteKey(id string) error {
	if strings.TrimSpace(id) == "" {
		return ErrNotFound
	}
	_, err := s.DeleteKeys([]string{id})
	return err
}

func (s *Store) RestoreKey(id string) (Key, error) {
	key, err := s.mutateKey(id, func(key *Key) {
		key.Paused = false
		key.Consecutive429 = 0
		key.RateLimitedUntil = time.Time{}
		key.ManualPaused = false
		key.BalanceDepleted = false
		key.ConsecutiveErrors = 0
		key.LastError = ""
	})
	if err != nil {
		return key, err
	}
	return key, s.Flush()
}

func (s *Store) RestoreAllKeys() (int, error) {
	return s.changeKeys(nil, func(key *Key) bool {
		if !key.ManualPaused && !key.Paused && !key.BalanceDepleted && key.Consecutive429 == 0 && key.RateLimitedUntil.IsZero() && key.ConsecutiveErrors == 0 && key.LastError == "" {
			return false
		}
		key.Consecutive429 = 0
		key.RateLimitedUntil = time.Time{}
		key.ManualPaused = false
		key.Paused = false
		key.BalanceDepleted = false
		key.ConsecutiveErrors = 0
		key.LastError = ""
		return true
	})
}

func (s *Store) RecordSuccess(id string, responseMS ...int64) error {
	_, err := s.mutateKey(id, func(key *Key) {
		reset429Sequence(key, time.Now().UTC())
		key.ConsecutiveErrors = 0
		key.LastError = ""
		key.LastUsedAt = time.Now().UTC()
		if len(responseMS) > 0 {
			value := responseMS[0]
			key.LastResponseMS = &value
		}
	})
	return err
}

func (s *Store) RecordFailure(id, message string, balanceDepleted bool, responseMS ...int64) (Key, error) {
	cfg, err := s.GlobalConfig()
	if err != nil {
		return Key{}, err
	}
	return s.mutateKey(id, func(key *Key) {
		reset429Sequence(key, time.Now().UTC())
		key.ConsecutiveErrors++
		key.LastError = message
		key.LastUsedAt = time.Now().UTC()
		if len(responseMS) > 0 {
			value := responseMS[0]
			key.LastResponseMS = &value
		}
		if balanceDepleted {
			key.BalanceDepleted = true
			key.Paused = true
		}
		if cfg.KeyAutoPauseEnabled && key.ConsecutiveErrors >= cfg.KeyAutoPauseThreshold {
			key.Paused = true
		}
	})
}

// Cooldown expiry is checked lazily; no capture scans or background writes.
func reset429Sequence(key *Key, now time.Time) {
	key.Consecutive429 = 0
	if !key.IsRateLimited(now) {
		key.RateLimitedUntil = time.Time{}
	}
}

// RecordNon429 is used by model-list requests, which do not record generation metrics.
func (s *Store) RecordNon429(id string, status int) error {
	_, err := s.mutateKey(id, func(key *Key) { reset429Sequence(key, time.Now().UTC()) }, status)
	return err
}

func (s *Store) RecordRateLimit(id, message string, responseMS int64) (Key, error) {
	cfg, err := s.GlobalConfig()
	if err != nil {
		return Key{}, err
	}
	return s.recordRateLimitAt(id, message, responseMS, cfg, time.Now().UTC())
}

func (s *Store) recordRateLimitAt(id, message string, responseMS int64, cfg GlobalConfig, now time.Time) (Key, error) {
	return s.mutateKey(id, func(key *Key) {
		key.LastError = message
		key.LastUsedAt = now
		key.LastResponseMS = &responseMS
		key.ConsecutiveErrors = 0
		if key.IsRateLimited(now) {
			return
		}
		if !key.RateLimitedUntil.IsZero() {
			reset429Sequence(key, now)
		}
		key.Consecutive429++
		if key.Consecutive429 >= cfg.Key429Threshold {
			key.RateLimitedUntil = now.Add(time.Duration(cfg.Key429CooldownMinutes) * time.Minute)
		}
	}, 429)
}

func (s *Store) SaveCapture(c Capture) error { return s.enqueueCapture(c) }

func (s *Store) RecordMetric(keyID string, sample MetricSample) error {
	now := time.Now().UTC()
	at := sample.Time.UTC()
	if at.IsZero() {
		at = now
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.pruneMetricsLocked(now)
	if at.Before(now.Add(-metricsMaxAge)) || at.After(now.Add(time.Minute)) {
		return nil
	}
	bucketTime := at.Truncate(time.Minute)
	id := string(metricBucketKey(bucketTime))
	record := s.metrics[id]
	record.Time = bucketTime
	record.Requests++
	if sample.FirstByteMS != nil {
		record.FirstByteTotalMS += *sample.FirstByteMS
		record.FirstByteSamples++
	}
	if sample.Concurrency > record.MaxConcurrency {
		record.MaxConcurrency = sample.Concurrency
	}
	if sample.QueueDepth > record.MaxQueueDepth {
		record.MaxQueueDepth = sample.QueueDepth
	}
	if sample.Error {
		record.Errors++
	}
	s.metrics[id] = record
	s.dirtyMetrics[id] = s.nextRevisionLocked()
	if sample.FirstByteMS != nil {
		if key, ok := s.keys[keyID]; ok {
			value := *sample.FirstByteMS
			key.LastFirstByteMS = &value
			key.UpdatedAt = now
			s.keys[keyID] = key
			s.dirtyKeys[keyID] = s.nextRevisionLocked()
		}
	}
	return nil
}

func (s *Store) ListMetrics(granularity string, from, to time.Time) ([]MetricPoint, error) {
	to = to.UTC()
	if to.IsZero() {
		to = time.Now().UTC()
	}
	from = from.UTC()
	retentionFloor := to.Add(-metricsMaxAge)
	if from.IsZero() || from.Before(retentionFloor) {
		from = retentionFloor
	}
	if from.After(to) {
		from = to
	}
	truncate := metricTruncator(granularity)
	aggregated := make(map[int64]metricBucketRecord)
	s.mu.RLock()
	records := make([]metricBucketRecord, 0, len(s.metrics))
	for _, record := range s.metrics {
		records = append(records, record)
	}
	s.mu.RUnlock()
	for _, record := range records {
		record.Time = record.Time.UTC()
		if record.Time.Before(from) || record.Time.After(to) {
			continue
		}
		pointTime := truncate(record.Time)
		key := pointTime.Unix()
		next := aggregated[key]
		if next.Time.IsZero() {
			next.Time = pointTime
		}
		next.Requests += record.Requests
		next.FirstByteTotalMS += record.FirstByteTotalMS
		next.FirstByteSamples += record.FirstByteSamples
		if record.MaxConcurrency > next.MaxConcurrency {
			next.MaxConcurrency = record.MaxConcurrency
		}
		if record.MaxQueueDepth > next.MaxQueueDepth {
			next.MaxQueueDepth = record.MaxQueueDepth
		}
		next.Errors += record.Errors
		aggregated[key] = next
	}
	points := make([]MetricPoint, 0, len(aggregated))
	for _, record := range aggregated {
		var firstByte *int64
		if record.FirstByteSamples > 0 {
			avg := record.FirstByteTotalMS / int64(record.FirstByteSamples)
			firstByte = &avg
		}
		points = append(points, MetricPoint{
			Time:        record.Time,
			FirstByteMS: firstByte,
			Concurrency: record.MaxConcurrency,
			QueueDepth:  record.MaxQueueDepth,
			Errors:      record.Errors,
			Requests:    record.Requests,
		})
	}
	sort.Slice(points, func(i, j int) bool {
		return points[i].Time.Before(points[j].Time)
	})
	return points, nil
}

func (s *Store) GetCapture(keyID string) (Capture, error) {
	s.mu.RLock()
	if _, ok := s.keys[keyID]; !ok {
		s.mu.RUnlock()
		return Capture{}, ErrNotFound
	}
	pending := s.pendingCaptures[keyID]
	if pending == nil {
		pending = s.flushingCaptures[keyID]
	}
	if pending != nil && pending.generation != s.keyGeneration[keyID] {
		pending = nil
	}
	ignore := s.ignoreStoredCapture[keyID]
	s.mu.RUnlock()
	var c Capture
	if pending != nil {
		err := json.Unmarshal(pending.raw, &c)
		return c, err
	}
	if ignore {
		return c, ErrNotFound
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte(captureBucket)).Get([]byte(keyID))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &c)
	})
	return c, err
}

func (s *Store) ListProxyNodes(now time.Time) ([]ProxyNodeView, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	s.mu.RLock()
	nodes := make([]ProxyNodeView, 0, len(s.proxies))
	for _, node := range s.proxies {
		nodes = append(nodes, proxyNodeView(node, now))
	}
	s.mu.RUnlock()
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes, nil
}

func (s *Store) GetProxyNode(id string) (ProxyNode, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	node, ok := s.proxies[id]
	if !ok {
		return ProxyNode{}, ErrNotFound
	}
	return node, nil
}

func (s *Store) UpsertProxyNodeReport(report ProxyNodeReport, remoteAddr string) (ProxyNode, error) {
	report.ID = strings.TrimSpace(report.ID)
	if report.ID == "" {
		return ProxyNode{}, fmt.Errorf("proxy node id is required")
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ProxyNode{}, ErrClosed
	}
	node, exists := s.proxies[report.ID]
	if !exists {
		node = ProxyNode{ID: report.ID, Name: strings.TrimSpace(report.Name), Enabled: true, MaxConcurrency: 100, CreatedAt: now}
		if node.Name == "" {
			node.Name = node.ID
		}
		if report.MaxConcurrency > 0 {
			node.MaxConcurrency = report.MaxConcurrency
		}
	}
	if name := strings.TrimSpace(report.Name); name != "" {
		node.Name = name
	}
	node.ProxyURL = strings.TrimSpace(report.ProxyURL)
	if node.ProxyURL == "" {
		node.ProxyURL = deriveProxyURL(remoteAddr, report.ListenAddr)
	}
	node.ListenAddr = strings.TrimSpace(report.ListenAddr)
	node.Version = strings.TrimSpace(report.Version)
	node.StartedAt = report.StartedAt.UTC()
	node.LastReportAt = now
	node.LastRemoteAddr = remoteHost(remoteAddr)
	node.CurrentConnections = nonNegativeInt(report.CurrentConnections)
	node.TotalRequests = nonNegativeInt64(report.TotalRequests)
	node.FailedRequests = nonNegativeInt64(report.FailedRequests)
	node.BytesIn = nonNegativeInt64(report.BytesIn)
	node.BytesOut = nonNegativeInt64(report.BytesOut)
	node.LastError = strings.TrimSpace(report.LastError)
	node.UpdatedAt = now
	if node.MaxConcurrency <= 0 {
		node.MaxConcurrency = 100
	}
	if err := ValidateProxyNode(node); err != nil {
		return ProxyNode{}, err
	}
	s.proxies[report.ID] = node
	s.dirtyProxies[report.ID] = s.nextRevisionLocked()
	return node, nil
}

func (s *Store) UpdateProxyNode(id string, patch ProxyNode) (ProxyNode, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ProxyNode{}, ErrClosed
	}
	node, ok := s.proxies[id]
	if !ok {
		s.mu.Unlock()
		return ProxyNode{}, ErrNotFound
	}
	node.Name = strings.TrimSpace(patch.Name)
	node.ProxyURL = strings.TrimSpace(patch.ProxyURL)
	node.Enabled = patch.Enabled
	node.MaxConcurrency = patch.MaxConcurrency
	if node.Name == "" {
		node.Name = node.ID
	}
	if node.MaxConcurrency <= 0 {
		node.MaxConcurrency = 100
	}
	node.UpdatedAt = time.Now().UTC()
	if err := ValidateProxyNode(node); err != nil {
		s.mu.Unlock()
		return ProxyNode{}, err
	}
	s.proxies[id] = node
	s.dirtyProxies[id] = s.nextRevisionLocked()
	s.mu.Unlock()
	return node, s.Flush()
}

func (s *Store) DeleteProxyNode(id string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	delete(s.proxies, id)
	s.dirtyProxies[id] = s.nextRevisionLocked()
	s.mu.Unlock()
	return s.Flush()
}

func proxyNodeView(node ProxyNode, now time.Time) ProxyNodeView {
	online := !node.LastReportAt.IsZero() && now.UTC().Sub(node.LastReportAt.UTC()) <= ProxyNodeOnlineAfter
	status := "offline"
	if !node.Enabled {
		status = "disabled"
	} else if online {
		status = "online"
	}
	return ProxyNodeView{ProxyNode: node, Online: online, Status: status}
}

func deriveProxyURL(remoteAddr, listenAddr string) string {
	host := remoteHost(remoteAddr)
	port := listenPort(listenAddr)
	if host == "" || port == "" {
		return ""
	}
	return "http://" + net.JoinHostPort(host, port)
}

func remoteHost(remoteAddr string) string {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remoteAddr))
	if err == nil {
		return strings.Trim(host, "[]")
	}
	return strings.Trim(strings.TrimSpace(remoteAddr), "[]")
}

func listenPort(listenAddr string) string {
	listenAddr = strings.TrimSpace(listenAddr)
	if listenAddr == "" {
		return "9070"
	}
	if _, port, err := net.SplitHostPort(listenAddr); err == nil {
		return port
	}
	if strings.HasPrefix(listenAddr, ":") {
		return strings.TrimPrefix(listenAddr, ":")
	}
	if strings.IndexByte(listenAddr, ':') < 0 {
		return listenAddr
	}
	return ""
}

func nonNegativeInt(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func nonNegativeInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func metricBucketKey(t time.Time) []byte {
	return []byte(fmt.Sprintf("%020d", t.UTC().Truncate(time.Minute).Unix()))
}

func deleteOldMetrics(b *bolt.Bucket, cutoff time.Time) error {
	cursor := b.Cursor()
	// Keys are ordered minute timestamps. Retain the boundary minute because
	// samples later in that minute may still be within the seven-day window.
	boundary := string(metricBucketKey(cutoff))
	// Deletion shifts leaf entries; seeking the first remaining key avoids
	// skipping the adjacent expired entry.
	for k, _ := cursor.First(); k != nil && string(k) < boundary; k, _ = cursor.First() {
		if err := cursor.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func metricTruncator(granularity string) func(time.Time) time.Time {
	switch strings.TrimSpace(granularity) {
	case "hour":
		return func(t time.Time) time.Time { return t.UTC().Truncate(time.Hour) }
	case "day":
		return func(t time.Time) time.Time {
			t = t.UTC()
			return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		}
	default:
		return func(t time.Time) time.Time { return t.UTC().Truncate(time.Minute) }
	}
}

// mutateKey updates the shared runtime state atomically so in-flight results
// cannot overwrite a concurrent manual pause or configuration edit.
func (s *Store) mutateKey(id string, mutate func(*Key), responseStatus ...int) (Key, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Key{}, ErrClosed
	}
	key, ok := s.keys[id]
	if !ok {
		return Key{}, ErrNotFound
	}
	key = cloneKey(key)
	mutate(&key)
	key.UpdatedAt = time.Now().UTC()
	if len(responseStatus) > 0 {
		key.LastStatusCode = responseStatus[0]
	}
	s.keys[id] = key
	s.dirtyKeys[id] = s.nextRevisionLocked()
	return cloneKey(key), nil
}

func ValidateKey(key Key) error {
	if strings.TrimSpace(key.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if strings.TrimSpace(key.APIKey) == "" {
		return fmt.Errorf("api key is required")
	}
	if err := validateOptionalURL(key.BaseURL, "base URL"); err != nil {
		return err
	}
	if err := validateOptionalURL(key.ProxyURL, "proxy URL"); err != nil {
		return err
	}
	if key.MaxConcurrency <= 0 {
		return fmt.Errorf("max concurrency must be greater than zero")
	}
	return nil
}

func ValidateProxyNode(node ProxyNode) error {
	if strings.TrimSpace(node.ID) == "" {
		return fmt.Errorf("proxy node id is required")
	}
	if !validProxyNodeID(node.ID) {
		return fmt.Errorf("proxy node id may only contain letters, numbers, dot, underscore, and dash")
	}
	if strings.TrimSpace(node.Name) == "" {
		return fmt.Errorf("proxy node name is required")
	}
	if err := validateOptionalURL(node.ProxyURL, "proxy URL"); err != nil {
		return err
	}
	if node.MaxConcurrency <= 0 {
		return fmt.Errorf("max concurrency must be greater than zero")
	}
	return nil
}

func validProxyNodeID(id string) bool {
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func validateRequiredURL(value, name string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	return validateURL(value, name)
}

func validateOptionalURL(value, name string) error {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return validateURL(value, name)
}

func validateURL(value, name string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fmt.Errorf("%s must be a valid URL", name)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("%s must use http or https", name)
	}
	return nil
}

func randomID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf[:])
}
