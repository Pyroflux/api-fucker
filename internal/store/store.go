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
	keysBucket       = "keys"
	metaBucket       = "meta"
	captureBucket    = "captures"
	metricsBucket    = "metrics"
	proxyNodesBucket = "proxy_nodes"
	globalConfigID   = "global"
	metricsMaxAge    = 7 * 24 * time.Hour

	ProxyModeRoundRobin  = "round_robin"
	ProxyModeKeyBinding  = "key_binding"
	ProxyNodeOnlineAfter = 30 * time.Second
)

var (
	ErrNotFound = errors.New("not found")
)

type Store struct {
	db *bolt.DB
}

type GlobalConfig struct {
	UpstreamBaseURL  string    `json:"upstreamBaseUrl"`
	DefaultProxyURL  string    `json:"defaultProxyUrl"`
	IPAllowlist      []string  `json:"ipAllowlist,omitempty"`
	IPBlocklist      []string  `json:"ipBlocklist,omitempty"`
	ProxyPoolEnabled bool      `json:"proxyPoolEnabled"`
	ProxyMode        string    `json:"proxyMode"`
	UpdatedAt        time.Time `json:"updatedAt"`
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
	BalanceDepleted   bool      `json:"balanceDepleted"`
	ConsecutiveErrors int       `json:"consecutiveErrors"`
	LastError         string    `json:"lastError"`
	LastFirstByteMS   *int64    `json:"lastFirstByteMs,omitempty"`
	LastUsedAt        time.Time `json:"lastUsedAt"`
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
}

type KeyView struct {
	Key
	CurrentConcurrency int `json:"currentConcurrency"`
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
	Error       bool
}

type metricBucketRecord struct {
	Time             time.Time `json:"time"`
	Requests         int       `json:"requests"`
	FirstByteTotalMS int64     `json:"firstByteTotalMs"`
	FirstByteSamples int       `json:"firstByteSamples"`
	MaxConcurrency   int       `json:"maxConcurrency"`
	Errors           int       `json:"errors"`
}

type MetricPoint struct {
	Time        time.Time `json:"time"`
	FirstByteMS *int64    `json:"firstByteMs,omitempty"`
	Concurrency int       `json:"concurrency"`
	Errors      int       `json:"errors"`
	Requests    int       `json:"requests"`
}

type Capture struct {
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
	return st, nil
}

func (s *Store) Close() error {
	return s.db.Close()
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
		if _, err := tx.CreateBucketIfNotExists([]byte(metricsBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(proxyNodesBucket)); err != nil {
			return err
		}
		b := tx.Bucket([]byte(metaBucket))
		if b.Get([]byte(globalConfigID)) == nil {
			cfg := GlobalConfig{UpdatedAt: time.Now().UTC()}
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
	var cfg GlobalConfig
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte(metaBucket)).Get([]byte(globalConfigID))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &cfg)
	})
	return cfg, err
}

func (s *Store) SaveGlobalConfig(cfg GlobalConfig) error {
	if err := validateRequiredURL(cfg.UpstreamBaseURL, "upstream base URL"); err != nil {
		return err
	}
	if err := validateOptionalURL(cfg.DefaultProxyURL, "default proxy URL"); err != nil {
		return err
	}
	cfg.ProxyMode = normalizeProxyMode(cfg.ProxyMode)
	cfg.IPAllowlist = normalizeRules(cfg.IPAllowlist)
	cfg.IPBlocklist = normalizeRules(cfg.IPBlocklist)
	cfg.UpdatedAt = time.Now().UTC()
	return s.db.Update(func(tx *bolt.Tx) error {
		raw, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		return tx.Bucket([]byte(metaBucket)).Put([]byte(globalConfigID), raw)
	})
}

func normalizeProxyMode(mode string) string {
	switch strings.TrimSpace(mode) {
	case ProxyModeKeyBinding:
		return ProxyModeKeyBinding
	default:
		return ProxyModeRoundRobin
	}
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
	keys := make([]Key, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(keysBucket)).ForEach(func(_, v []byte) error {
			var key Key
			if err := json.Unmarshal(v, &key); err != nil {
				return err
			}
			keys = append(keys, key)
			return nil
		})
	})
	return keys, err
}

func (s *Store) GetKey(id string) (Key, error) {
	var key Key
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte(keysBucket)).Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &key)
	})
	return key, err
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
	if !key.Enabled {
		key.Enabled = true
	}
	if err := ValidateKey(key); err != nil {
		return Key{}, err
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(keysBucket))
		if b.Get([]byte(key.ID)) != nil {
			return fmt.Errorf("key id already exists")
		}
		raw, err := json.Marshal(key)
		if err != nil {
			return err
		}
		return b.Put([]byte(key.ID), raw)
	})
	return key, err
}

func (s *Store) UpdateKey(id string, patch Key) (Key, error) {
	var updated Key
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(keysBucket))
		raw := b.Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		if err := json.Unmarshal(raw, &updated); err != nil {
			return err
		}
		updated.Name = patch.Name
		updated.APIKey = patch.APIKey
		updated.BaseURL = patch.BaseURL
		updated.ProxyURL = patch.ProxyURL
		updated.MaxConcurrency = patch.MaxConcurrency
		updated.Enabled = patch.Enabled
		updated.Paused = patch.Paused
		updated.BalanceDepleted = patch.BalanceDepleted
		updated.UpdatedAt = time.Now().UTC()
		if updated.MaxConcurrency <= 0 {
			updated.MaxConcurrency = 1
		}
		if err := ValidateKey(updated); err != nil {
			return err
		}
		next, err := json.Marshal(updated)
		if err != nil {
			return err
		}
		return b.Put([]byte(id), next)
	})
	return updated, err
}

func (s *Store) UpdateKeysMaxConcurrency(ids []string, maxConcurrency int) (BulkUpdateResult, error) {
	if maxConcurrency <= 0 {
		return BulkUpdateResult{}, fmt.Errorf("max concurrency must be greater than zero")
	}
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			wanted[id] = true
		}
	}
	updateAll := len(wanted) == 0
	result := BulkUpdateResult{}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(keysBucket))
		type keyUpdate struct {
			id  []byte
			key Key
		}
		updates := make([]keyUpdate, 0)
		if err := b.ForEach(func(k, v []byte) error {
			id := string(k)
			if !updateAll && !wanted[id] {
				return nil
			}
			var key Key
			if err := json.Unmarshal(v, &key); err != nil {
				return err
			}
			key.MaxConcurrency = maxConcurrency
			key.UpdatedAt = time.Now().UTC()
			if err := ValidateKey(key); err != nil {
				return err
			}
			updates = append(updates, keyUpdate{id: append([]byte(nil), k...), key: key})
			return nil
		}); err != nil {
			return err
		}
		for _, update := range updates {
			raw, err := json.Marshal(update.key)
			if err != nil {
				return err
			}
			if err := b.Put(update.id, raw); err != nil {
				return err
			}
			result.Updated++
		}
		return nil
	})
	return result, err
}

func (s *Store) DeleteKeys(ids []string) (BulkDeleteResult, error) {
	wanted := make(map[string]bool, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			wanted[id] = true
		}
	}
	deleteAll := len(wanted) == 0
	result := BulkDeleteResult{}
	err := s.db.Update(func(tx *bolt.Tx) error {
		keys := tx.Bucket([]byte(keysBucket))
		captures := tx.Bucket([]byte(captureBucket))
		toDelete := make([][]byte, 0)
		if err := keys.ForEach(func(k, _ []byte) error {
			id := string(k)
			if deleteAll || wanted[id] {
				toDelete = append(toDelete, append([]byte(nil), k...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, id := range toDelete {
			if err := keys.Delete(id); err != nil {
				return err
			}
			if err := captures.Delete(id); err != nil {
				return err
			}
			result.Deleted++
		}
		return nil
	})
	return result, err
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
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte(keysBucket)).Delete([]byte(id)); err != nil {
			return err
		}
		return tx.Bucket([]byte(captureBucket)).Delete([]byte(id))
	})
}

func (s *Store) RestoreKey(id string) (Key, error) {
	key, err := s.GetKey(id)
	if err != nil {
		return Key{}, err
	}
	key.Paused = false
	key.BalanceDepleted = false
	key.ConsecutiveErrors = 0
	key.LastError = ""
	return s.saveKey(key)
}

func (s *Store) RestoreAllKeys() (int, error) {
	restored := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(keysBucket))
		return b.ForEach(func(id, raw []byte) error {
			var key Key
			if err := json.Unmarshal(raw, &key); err != nil {
				return err
			}
			if !key.Paused && !key.BalanceDepleted && key.ConsecutiveErrors == 0 && key.LastError == "" {
				return nil
			}
			key.Paused = false
			key.BalanceDepleted = false
			key.ConsecutiveErrors = 0
			key.LastError = ""
			key.UpdatedAt = time.Now().UTC()
			next, err := json.Marshal(key)
			if err != nil {
				return err
			}
			if err := b.Put(id, next); err != nil {
				return err
			}
			restored++
			return nil
		})
	})
	return restored, err
}

func (s *Store) RecordSuccess(id string) error {
	key, err := s.GetKey(id)
	if err != nil {
		return err
	}
	key.ConsecutiveErrors = 0
	key.LastError = ""
	key.LastUsedAt = time.Now().UTC()
	_, err = s.saveKey(key)
	return err
}

func (s *Store) RecordFailure(id, message string, balanceDepleted bool) (Key, error) {
	key, err := s.GetKey(id)
	if err != nil {
		return Key{}, err
	}
	key.ConsecutiveErrors++
	key.LastError = message
	key.LastUsedAt = time.Now().UTC()
	if balanceDepleted {
		key.BalanceDepleted = true
		key.Paused = true
	}
	if key.ConsecutiveErrors >= 3 {
		key.Paused = true
	}
	return s.saveKey(key)
}

func (s *Store) SaveCapture(c Capture) error {
	c.Time = c.Time.UTC()
	return s.db.Update(func(tx *bolt.Tx) error {
		raw, err := json.Marshal(c)
		if err != nil {
			return err
		}
		return tx.Bucket([]byte(captureBucket)).Put([]byte(c.KeyID), raw)
	})
}

func (s *Store) RecordMetric(keyID string, sample MetricSample) error {
	now := sample.Time.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	bucketTime := now.Truncate(time.Minute)
	return s.db.Update(func(tx *bolt.Tx) error {
		metrics := tx.Bucket([]byte(metricsBucket))
		if err := deleteOldMetrics(metrics, now.Add(-metricsMaxAge)); err != nil {
			return err
		}

		record := metricBucketRecord{Time: bucketTime}
		metricKey := metricBucketKey(bucketTime)
		if raw := metrics.Get(metricKey); raw != nil {
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
		}
		record.Requests++
		if sample.FirstByteMS != nil {
			record.FirstByteTotalMS += *sample.FirstByteMS
			record.FirstByteSamples++
		}
		if sample.Concurrency > record.MaxConcurrency {
			record.MaxConcurrency = sample.Concurrency
		}
		if sample.Error {
			record.Errors++
		}
		raw, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if err := metrics.Put(metricKey, raw); err != nil {
			return err
		}
		if sample.FirstByteMS == nil || strings.TrimSpace(keyID) == "" {
			return nil
		}
		return updateKeyFirstByte(tx, keyID, *sample.FirstByteMS, now)
	})
}

func (s *Store) ListMetrics(granularity string, now time.Time) ([]MetricPoint, error) {
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	start := now.Add(-metricsMaxAge)
	truncate := metricTruncator(granularity)
	aggregated := make(map[int64]metricBucketRecord)
	err := s.db.View(func(tx *bolt.Tx) error {
		metrics := tx.Bucket([]byte(metricsBucket))
		return metrics.ForEach(func(_, raw []byte) error {
			var record metricBucketRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			record.Time = record.Time.UTC()
			if record.Time.Before(start) || record.Time.After(now) {
				return nil
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
			next.Errors += record.Errors
			aggregated[key] = next
			return nil
		})
	})
	if err != nil {
		return nil, err
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
	var c Capture
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
	nodes := make([]ProxyNodeView, 0)
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(proxyNodesBucket)).ForEach(func(_, raw []byte) error {
			var node ProxyNode
			if err := json.Unmarshal(raw, &node); err != nil {
				return err
			}
			nodes = append(nodes, proxyNodeView(node, now))
			return nil
		})
	})
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes, err
}

func (s *Store) GetProxyNode(id string) (ProxyNode, error) {
	var node ProxyNode
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte(proxyNodesBucket)).Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		return json.Unmarshal(raw, &node)
	})
	return node, err
}

func (s *Store) UpsertProxyNodeReport(report ProxyNodeReport, remoteAddr string) (ProxyNode, error) {
	report.ID = strings.TrimSpace(report.ID)
	if report.ID == "" {
		return ProxyNode{}, fmt.Errorf("proxy node id is required")
	}
	now := time.Now().UTC()
	var node ProxyNode
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(proxyNodesBucket))
		raw := b.Get([]byte(report.ID))
		if raw == nil {
			node = ProxyNode{
				ID:             report.ID,
				Name:           strings.TrimSpace(report.Name),
				Enabled:        true,
				MaxConcurrency: 100,
				CreatedAt:      now,
			}
			if node.Name == "" {
				node.Name = node.ID
			}
			if report.MaxConcurrency > 0 {
				node.MaxConcurrency = report.MaxConcurrency
			}
		} else if err := json.Unmarshal(raw, &node); err != nil {
			return err
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
			return err
		}
		next, err := json.Marshal(node)
		if err != nil {
			return err
		}
		return b.Put([]byte(node.ID), next)
	})
	return node, err
}

func (s *Store) UpdateProxyNode(id string, patch ProxyNode) (ProxyNode, error) {
	var node ProxyNode
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(proxyNodesBucket))
		raw := b.Get([]byte(id))
		if raw == nil {
			return ErrNotFound
		}
		if err := json.Unmarshal(raw, &node); err != nil {
			return err
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
			return err
		}
		next, err := json.Marshal(node)
		if err != nil {
			return err
		}
		return b.Put([]byte(id), next)
	})
	return node, err
}

func (s *Store) DeleteProxyNode(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(proxyNodesBucket)).Delete([]byte(id))
	})
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

func updateKeyFirstByte(tx *bolt.Tx, id string, firstByteMS int64, now time.Time) error {
	b := tx.Bucket([]byte(keysBucket))
	raw := b.Get([]byte(id))
	if raw == nil {
		return nil
	}
	var key Key
	if err := json.Unmarshal(raw, &key); err != nil {
		return err
	}
	key.LastFirstByteMS = &firstByteMS
	key.UpdatedAt = now.UTC()
	next, err := json.Marshal(key)
	if err != nil {
		return err
	}
	return b.Put([]byte(id), next)
}

func metricBucketKey(t time.Time) []byte {
	return []byte(fmt.Sprintf("%020d", t.UTC().Truncate(time.Minute).Unix()))
}

func deleteOldMetrics(b *bolt.Bucket, cutoff time.Time) error {
	c := b.Cursor()
	for k, raw := c.First(); k != nil; k, raw = c.Next() {
		var record metricBucketRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return err
		}
		if !record.Time.Before(cutoff) {
			continue
		}
		if err := c.Delete(); err != nil {
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

func (s *Store) saveKey(key Key) (Key, error) {
	key.UpdatedAt = time.Now().UTC()
	if err := ValidateKey(key); err != nil {
		return Key{}, err
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		raw, err := json.Marshal(key)
		if err != nil {
			return err
		}
		return tx.Bucket([]byte(keysBucket)).Put([]byte(key.ID), raw)
	})
	return key, err
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
