package store

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var ErrClosed = errors.New("store closed")

// Runtime state never holds mu across database I/O. Only persistenceLoop writes
// the database after startup; revisions keep in-flight snapshots from clearing
// newer updates. Automatic updates are deliberately best-effort on crash.
type runtimeState struct {
	latestCaptureSequence                 map[string]uint64
	mu                                    sync.RWMutex
	config                                GlobalConfig
	keys                                  map[string]Key
	keyOrder                              []string
	proxies                               map[string]ProxyNode
	metrics                               map[string]metricBucketRecord
	revision                              uint64
	dirtyConfig                           uint64
	dirtyKeys, dirtyProxies, dirtyMetrics map[string]uint64
	dirtyCaptureDeletes                   map[string]uint64
	keyGeneration                         map[string]uint64
	ignoreStoredCapture                   map[string]bool
	pendingCaptures, flushingCaptures     map[string]*queuedCapture
	captureBytes                          int
	droppedCaptures                       uint64
	lastMetricPrune                       time.Time
	lastDiskPrune                         time.Time // writer-owned
	lastPersistAt                         time.Time
	lastPersistError                      string
	flushRequests                         chan chan error
	stop                                  chan struct{}
	done                                  chan struct{}
	closed                                bool
	closeErr                              error
}

type PersistenceHealth struct {
	PendingKeys         int       `json:"pendingKeys"`
	PendingNodes        int       `json:"pendingNodes"`
	PendingMetrics      int       `json:"pendingMetrics"`
	PendingCaptures     int       `json:"pendingCaptures"`
	PendingCaptureBytes int       `json:"pendingCaptureBytes"`
	DroppedCaptures     uint64    `json:"droppedCaptures"`
	LastSuccessAt       time.Time `json:"lastSuccessAt"`
	LastError           string    `json:"lastError"`
}

func (s *Store) PersistenceHealth() PersistenceHealth {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return PersistenceHealth{len(s.dirtyKeys), len(s.dirtyProxies), len(s.dirtyMetrics), len(s.pendingCaptures) + len(s.flushingCaptures), s.captureBytes, s.droppedCaptures, s.lastPersistAt, s.lastPersistError}
}

func (s *Store) loadRuntime() error {
	s.runtimeState = &runtimeState{
		latestCaptureSequence: make(map[string]uint64),
		keys:                  make(map[string]Key), proxies: make(map[string]ProxyNode), metrics: make(map[string]metricBucketRecord),
		dirtyCaptureDeletes: make(map[string]uint64), dirtyKeys: make(map[string]uint64), dirtyProxies: make(map[string]uint64), dirtyMetrics: make(map[string]uint64),
		keyGeneration: make(map[string]uint64), ignoreStoredCapture: make(map[string]bool),
		pendingCaptures: make(map[string]*queuedCapture), flushingCaptures: make(map[string]*queuedCapture),
		flushRequests: make(chan chan error), stop: make(chan struct{}), done: make(chan struct{}),
	}
	now := time.Now().UTC()
	return s.db.View(func(tx *bolt.Tx) error {
		if err := json.Unmarshal(tx.Bucket([]byte(metaBucket)).Get([]byte(globalConfigID)), &s.config); err != nil {
			return err
		}
		s.config = normalizeGlobalConfig(s.config)
		s.config.revision = s.nextRevisionLocked()
		if err := tx.Bucket([]byte(keysBucket)).ForEach(func(id, raw []byte) error {
			var key Key
			if err := json.Unmarshal(raw, &key); err != nil {
				return err
			}
			// Only the small status index is read. Never decode historical captures.
			key.LastStatusCode = 0
			if status := tx.Bucket([]byte(captureStatusBucket)).Get(id); status != nil {
				if err := json.Unmarshal(status, &key.LastStatusCode); err != nil {
					return err
				}
			}
			s.keys[string(id)] = key
			s.keyOrder = append(s.keyOrder, string(id))
			s.keyGeneration[string(id)] = s.nextRevisionLocked()
			return nil
		}); err != nil {
			return err
		}
		if err := tx.Bucket([]byte(proxyNodesBucket)).ForEach(func(id, raw []byte) error {
			var node ProxyNode
			if err := json.Unmarshal(raw, &node); err != nil {
				return err
			}
			node.LastReportAt = time.Time{}
			node.CurrentConnections = 0
			s.proxies[string(id)] = node
			return nil
		}); err != nil {
			return err
		}
		c := tx.Bucket([]byte(metricsBucket)).Cursor()
		for id, raw := c.Seek(metricBucketKey(now.Add(-metricsMaxAge))); id != nil; id, raw = c.Next() {
			var record metricBucketRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				return err
			}
			if !record.Time.After(now.Add(time.Minute)) {
				s.metrics[string(id)] = record
			}
		}
		s.lastMetricPrune = now
		return nil
	})
}

func cloneConfig(cfg GlobalConfig) GlobalConfig {
	cfg.IPAllowlist = append([]string(nil), cfg.IPAllowlist...)
	cfg.IPBlocklist = append([]string(nil), cfg.IPBlocklist...)
	return cfg
}
func cloneKey(key Key) Key {
	if key.LastFirstByteMS != nil {
		v := *key.LastFirstByteMS
		key.LastFirstByteMS = &v
	}
	if key.LastResponseMS != nil {
		v := *key.LastResponseMS
		key.LastResponseMS = &v
	}
	return key
}
func (s *Store) nextRevisionLocked() uint64 { s.revision++; return s.revision }
func idSet(ids []string) map[string]bool {
	result := make(map[string]bool, len(ids))
	for _, id := range ids {
		result[id] = true
	}
	return result
}

// SelectKeyAfter visits stable IDs starting after the last admitted key. The
// predicate must not call Store methods or retain/mutate the borrowed key.
func (s *Store) SelectKeyAfter(after string, eligible func(Key) bool) (Key, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	start := sort.Search(len(s.keyOrder), func(i int) bool { return s.keyOrder[i] > after })
	for offset := 0; offset < len(s.keyOrder); offset++ {
		key := s.keys[s.keyOrder[(start+offset)%len(s.keyOrder)]]
		if eligible(key) {
			return cloneKey(key), true
		}
	}
	return Key{}, false
}

func (s *Store) changeKeys(ids []string, change func(*Key) bool) (int, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, ErrClosed
	}
	wanted := idSet(ids)
	count := 0
	now := time.Now().UTC()
	for id, key := range s.keys {
		if len(wanted) != 0 && !wanted[id] {
			continue
		}
		key = cloneKey(key)
		if !change(&key) {
			continue
		}
		key.UpdatedAt = now
		s.keys[id] = key
		s.dirtyKeys[id] = s.nextRevisionLocked()
		count++
	}
	s.mu.Unlock()
	return count, s.Flush()
}

func (s *Store) pruneMetricsLocked(now time.Time) {
	if now.Sub(s.lastMetricPrune) < time.Minute {
		return
	}
	cutoff := now.Add(-metricsMaxAge).Truncate(time.Minute)
	for id, record := range s.metrics {
		if record.Time.Before(cutoff) {
			delete(s.metrics, id)
			delete(s.dirtyMetrics, id)
		}
	}
	s.lastMetricPrune = now
}

// Flush confirms current metadata is durable. Captures are lower priority and
// may still be pending. A blocked disk cannot hold the runtime mutex.
func (s *Store) Flush() error {
	reply := make(chan error, 1)
	select {
	case s.flushRequests <- reply:
	case <-s.stop:
		return ErrClosed
	}
	select {
	case err := <-reply:
		return err
	case <-s.done:
		return ErrClosed
	}
}

func (s *Store) closeRuntime() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.CloseContext(ctx)
}

func (s *Store) CloseContext(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.stop)
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return s.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) persistenceLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(s.done)
	for {
		// Give waiting manual changes priority over diagnostic batches.
		select {
		case reply := <-s.flushRequests:
			reply <- s.persistMetadata()
			continue
		default:
		}
		select {
		case <-s.stop:
			s.closeErr = s.persistMetadata()
			if s.closeErr == nil {
				for {
					s.mu.RLock()
					n := len(s.pendingCaptures)
					s.mu.RUnlock()
					if n == 0 {
						break
					}
					if err := s.persistCaptures(); err != nil {
						s.closeErr = err
						break
					}
				}
			}
			if err := s.db.Close(); s.closeErr == nil {
				s.closeErr = err
			}
			return
		case reply := <-s.flushRequests:
			reply <- s.persistMetadata()
		case <-ticker.C:
			if s.persistMetadata() == nil {
				// At most one bounded capture transaction per interval. Metadata and
				// manual changes are serviced before the next batch.
				select {
				case reply := <-s.flushRequests:
					reply <- s.persistMetadata()
				default:
				}
				_ = s.persistCaptures()
			}
		}
	}
}

type metadataSnapshot struct {
	config                        GlobalConfig
	configRevision                uint64
	keys                          map[string]Key
	nodes                         map[string]ProxyNode
	metrics                       map[string]metricBucketRecord
	keyRevs, nodeRevs, metricRevs map[string]uint64
	captureDeletes                map[string]uint64
}

func copyRevisions(src map[string]uint64) map[string]uint64 {
	dst := make(map[string]uint64, len(src))
	for id, rev := range src {
		dst[id] = rev
	}
	return dst
}
func clearRevisions(current, saved map[string]uint64) {
	for id, rev := range saved {
		if current[id] == rev {
			delete(current, id)
		}
	}
}
func (s *Store) persistMetadata() error {
	now := time.Now().UTC()
	s.mu.Lock()
	s.pruneMetricsLocked(now)
	snap := metadataSnapshot{captureDeletes: copyRevisions(s.dirtyCaptureDeletes), config: s.config, configRevision: s.dirtyConfig,
		keys: make(map[string]Key, len(s.dirtyKeys)), nodes: make(map[string]ProxyNode, len(s.dirtyProxies)), metrics: make(map[string]metricBucketRecord, len(s.dirtyMetrics)),
		keyRevs: copyRevisions(s.dirtyKeys), nodeRevs: copyRevisions(s.dirtyProxies), metricRevs: copyRevisions(s.dirtyMetrics)}
	for id := range snap.keyRevs {
		if key, ok := s.keys[id]; ok {
			snap.keys[id] = key
		}
	}
	for id := range snap.nodeRevs {
		if node, ok := s.proxies[id]; ok {
			snap.nodes[id] = node
		}
	}
	for id := range snap.metricRevs {
		if record, ok := s.metrics[id]; ok {
			snap.metrics[id] = record
		}
	}
	s.mu.Unlock()
	prune := now.Sub(s.lastDiskPrune) >= time.Minute
	if snap.configRevision == 0 && len(snap.keyRevs)+len(snap.nodeRevs)+len(snap.metricRevs) == 0 && !prune {
		return nil
	}
	// JSON encoding and fsync both happen without the runtime mutex.
	err := s.db.Update(func(tx *bolt.Tx) error {
		put := func(bucket, id string, v any) error {
			raw, err := json.Marshal(v)
			if err != nil {
				return err
			}
			return tx.Bucket([]byte(bucket)).Put([]byte(id), raw)
		}
		if snap.configRevision != 0 {
			if err := put(metaBucket, globalConfigID, snap.config); err != nil {
				return err
			}
		}
		for id := range snap.captureDeletes {
			if err := tx.Bucket([]byte(captureBucket)).Delete([]byte(id)); err != nil {
				return err
			}
		}
		for id := range snap.keyRevs {
			key, ok := snap.keys[id]
			if !ok {
				for _, bucket := range []string{keysBucket, captureBucket, captureStatusBucket} {
					if err := tx.Bucket([]byte(bucket)).Delete([]byte(id)); err != nil {
						return err
					}
				}
			} else {
				if err := put(keysBucket, id, key); err != nil {
					return err
				}
				if err := put(captureStatusBucket, id, key.LastStatusCode); err != nil {
					return err
				}
			}
		}
		for id := range snap.nodeRevs {
			if node, ok := snap.nodes[id]; ok {
				if err := put(proxyNodesBucket, id, node); err != nil {
					return err
				}
			} else if err := tx.Bucket([]byte(proxyNodesBucket)).Delete([]byte(id)); err != nil {
				return err
			}
		}
		for id := range snap.metricRevs {
			if record, ok := snap.metrics[id]; ok {
				if err := put(metricsBucket, id, record); err != nil {
					return err
				}
			}
		}
		if prune {
			return deleteOldMetrics(tx.Bucket([]byte(metricsBucket)), now.Add(-metricsMaxAge))
		}
		return nil
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordPersistResultLocked(err)
	if err == nil {
		if s.dirtyConfig == snap.configRevision {
			s.dirtyConfig = 0
		}
		clearRevisions(s.dirtyCaptureDeletes, snap.captureDeletes)
		clearRevisions(s.dirtyKeys, snap.keyRevs)
		clearRevisions(s.dirtyProxies, snap.nodeRevs)
		clearRevisions(s.dirtyMetrics, snap.metricRevs)
		if prune {
			s.lastDiskPrune = now
		}
	}
	return err
}
func (s *Store) recordPersistResultLocked(err error) {
	if err != nil {
		s.lastPersistError = err.Error()
	} else {
		s.lastPersistError = ""
		s.lastPersistAt = time.Now().UTC()
	}
}
