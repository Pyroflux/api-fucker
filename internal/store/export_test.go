package store

import (
	"fmt"
	"sync"
	"time"
)

// Test-only access for the external integration suite using the real gateway.
func HoldWriterForTest(s *Store) (func(), error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, err
	}
	var once sync.Once
	return func() { once.Do(func() { _ = tx.Rollback() }) }, nil
}

func SeedRuntimeForTest(s *Store, keyCount int) error {
	s.mu.Lock()
	for i := 0; i < keyCount; i++ {
		id := fmt.Sprintf("key-%05d", i)
		s.keys[id] = Key{ID: id, Name: id, APIKey: "test-placeholder", Enabled: true, MaxConcurrency: 1}
		s.keyOrder = append(s.keyOrder, id)
		rev := s.nextRevisionLocked()
		s.dirtyKeys[id] = rev
		s.keyGeneration[id] = rev
	}
	now := time.Now().UTC().Truncate(time.Minute)
	for i := 0; i < 7*24*60; i++ {
		at := now.Add(-time.Duration(i) * time.Minute)
		id := string(metricBucketKey(at))
		s.metrics[id] = metricBucketRecord{Time: at, Requests: 1}
		s.dirtyMetrics[id] = s.nextRevisionLocked()
	}
	s.mu.Unlock()
	return s.Flush()
}
