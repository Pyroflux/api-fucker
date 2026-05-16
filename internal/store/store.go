package store

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	keysBucket     = "keys"
	metaBucket     = "meta"
	captureBucket  = "captures"
	globalConfigID = "global"
)

var (
	ErrNotFound = errors.New("not found")
)

type Store struct {
	db *bolt.DB
}

type GlobalConfig struct {
	UpstreamBaseURL string    `json:"upstreamBaseUrl"`
	DefaultProxyURL string    `json:"defaultProxyUrl"`
	IPAllowlist     []string  `json:"ipAllowlist,omitempty"`
	IPBlocklist     []string  `json:"ipBlocklist,omitempty"`
	UpdatedAt       time.Time `json:"updatedAt"`
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
