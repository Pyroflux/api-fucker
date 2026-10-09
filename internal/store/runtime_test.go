package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestCaptureBudgetAndCompletionOrder(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := SeedRuntimeForTest(st, 200); err != nil {
		t.Fatal(err)
	}
	release, err := HoldWriterForTest(st)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	old := st.PrepareCapture(Capture{KeyID: "key-00000", StatusCode: 429, ResponseBody: "old"})
	latest := st.PrepareCapture(Capture{KeyID: "key-00000", StatusCode: 200, ResponseBody: "new"})
	if err := st.SaveCapture(latest); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveCapture(old); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetCapture("key-00000")
	if err != nil || got.ResponseBody != "new" {
		t.Fatalf("stale completion: %+v %v", got, err)
	}
	large := strings.Repeat("\x00", 2*1024*1024)
	for i := 1; i < 200; i++ {
		if err := st.SaveCapture(Capture{KeyID: fmt.Sprintf("key-%05d", i), StatusCode: 201, RequestBody: large, ResponseBody: large}); err != nil {
			t.Fatal(err)
		}
	}
	health := st.PersistenceHealth()
	if health.PendingCaptures > captureQueueLimit || health.PendingCaptureBytes > captureQueueBytes || health.DroppedCaptures == 0 {
		t.Fatalf("unbounded queue: %+v", health)
	}
	st.mu.RLock()
	for _, item := range st.pendingCaptures {
		if len(item.raw) > captureRecordLimit {
			t.Errorf("capture too large: %d", len(item.raw))
		}
	}
	st.mu.RUnlock()
	got, err = st.GetCapture("key-00199")
	if err != nil || !got.Truncated || len(got.ResponseBody) > CaptureBodyLimit+256 {
		t.Fatalf("capture bound: truncated=%v bytes=%d err=%v", got.Truncated, len(got.ResponseBody), err)
	}
	key, _ := st.GetKey("key-00001")
	if key.LastStatusCode != 201 {
		t.Fatal("dropped capture lost status")
	}
	release()
}

func TestBlockedSnapshotPreservesManualChangeAndDeletion(t *testing.T) {
	path := t.TempDir() + "/data.db"
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	key, err := st.CreateKey(Key{ID: "key", Name: "key", APIKey: "test-placeholder"})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.RecordSuccess(key.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveCapture(Capture{KeyID: key.ID, ResponseBody: "old"}); err != nil {
		t.Fatal(err)
	}
	release, err := HoldWriterForTest(st)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() { done <- st.Flush() }()
	time.Sleep(30 * time.Millisecond)
	pauseDone := make(chan error, 1)
	go func() { _, err := st.PauseKeys([]string{key.ID}); pauseDone <- err }()
	deadline := time.Now().Add(time.Second)
	for {
		k, _ := st.GetKey(key.ID)
		if k.ManualPaused {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pause waited for disk before updating runtime")
		}
		time.Sleep(time.Millisecond)
	}
	if err := st.RecordSuccess(key.ID); err != nil {
		t.Fatal(err)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-pauseDone; err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	saved, _ := st.GetKey(key.ID)
	if !saved.ManualPaused {
		t.Fatal("stale snapshot undid manual pause")
	}
	if err := st.SaveCapture(Capture{KeyID: key.ID, ResponseBody: "must disappear"}); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteKey(key.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateKey(key); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetCapture(key.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted diagnostic resurrected: %v", err)
	}
}

func TestRestartWaitsForFreshHeartbeatAndFlushesState(t *testing.T) {
	path := t.TempDir() + "/data.db"
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.UpsertProxyNodeReport(ProxyNodeReport{ID: "node", ListenAddr: ":9080", CurrentConnections: 5}, "127.0.0.1:1234"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	nodes, _ := st.ListProxyNodes(time.Now())
	if len(nodes) != 1 || nodes[0].Online || nodes[0].CurrentConnections != 0 {
		t.Fatalf("stale online node after restart: %+v", nodes)
	}
	if _, err := st.UpsertProxyNodeReport(ProxyNodeReport{ID: "node", ListenAddr: ":9080"}, "127.0.0.1:1234"); err != nil {
		t.Fatal(err)
	}
	nodes, _ = st.ListProxyNodes(time.Now())
	if !nodes[0].Online {
		t.Fatal("fresh report did not restore eligibility")
	}
}

func TestPersistenceFailureRetainsDirtyState(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	key, err := st.CreateKey(Key{Name: "test", APIKey: "test-placeholder"})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an unavailable database; automatic runtime updates must survive.
	if err := st.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordSuccess(key.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err == nil {
		t.Fatal("disk failure was hidden")
	}
	health := st.PersistenceHealth()
	if health.PendingKeys != 1 || health.LastError == "" {
		t.Fatalf("failed write discarded state: %+v", health)
	}
	// Even a failed writer must not prevent first-time heartbeat registration.
	if _, err := st.UpsertProxyNodeReport(ProxyNodeReport{ID: "new", ListenAddr: ":9080"}, "127.0.0.1:1234"); err != nil {
		t.Fatal(err)
	}
}

func TestCloseDeadlineDoesNotWaitForeverForDisk(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	release, err := HoldWriterForTest(st)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := st.CloseContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close deadline: %v", err)
	}
	release()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMetricFlushUsesAbsoluteCountsAndPrunesPrefix(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.db.Update(func(tx *bolt.Tx) error {
		raw, _ := json.Marshal(metricBucketRecord{Time: time.Now().Add(-8 * 24 * time.Hour), Requests: 99})
		return tx.Bucket([]byte(metricsBucket)).Put(metricBucketKey(time.Now().Add(-8*24*time.Hour)), raw)
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 512; i++ {
		if err := st.RecordMetric("", MetricSample{Time: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(metricsBucket)).ForEach(func(_, raw []byte) error {
			var m metricBucketRecord
			if err := json.Unmarshal(raw, &m); err != nil {
				return err
			}
			if m.Requests != 512 {
				return fmt.Errorf("incorrect saved counts: %d", m.Requests)
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestMetricPrefixPruneDeletesAdjacentExpiredEntries(t *testing.T) {
	db, err := bolt.Open(t.TempDir()+"/metrics.db", 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now().UTC().Truncate(time.Minute)
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte(metricsBucket))
		if err != nil {
			return err
		}
		for i := 1; i <= 1500; i++ {
			if err := b.Put(metricBucketKey(now.Add(-metricsMaxAge-time.Duration(i)*time.Minute)), []byte("expired payload need not be decoded")); err != nil {
				return err
			}
		}
		if err := b.Put(metricBucketKey(now), []byte("keep")); err != nil {
			return err
		}
		if err := deleteOldMetrics(b, now.Add(-metricsMaxAge)); err != nil {
			return err
		}
		count := 0
		if err := b.ForEach(func(id, raw []byte) error {
			count++
			if string(raw) != "keep" {
				return fmt.Errorf("expired entry skipped: %s", id)
			}
			return nil
		}); err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("retained entries: %d", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
