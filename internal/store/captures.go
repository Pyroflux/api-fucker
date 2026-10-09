package store

import (
	"encoding/json"
	"strings"

	bolt "go.etcd.io/bbolt"
)

const (
	CaptureBodyLimit        = 64 * 1024
	captureRecordLimit      = 1024 * 1024
	captureQueueLimit       = 128
	captureQueueBytes       = 64 * 1024 * 1024
	captureBatchBytes       = 4 * 1024 * 1024
	CaptureTruncationMarker = "\n[diagnostic truncated]"
)

type queuedCapture struct {
	raw        []byte
	sequence   uint64
	generation uint64
}

// Bound both JSON size and retained backing strings. The 150 KiB content
// budget fits below 1 MiB even when JSON escapes every byte as six characters.
func boundedCapture(c Capture) Capture {
	remaining := 150 * 1024
	cut := func(v string, limit int) string {
		if limit > remaining {
			limit = remaining
		}
		if len(v) > limit {
			v = v[:limit]
			c.Truncated = true
		}
		remaining -= len(v)
		return strings.Clone(v)
	}
	c.KeyID = cut(c.KeyID, 4096)
	c.RequestMethod = cut(c.RequestMethod, 64)
	c.RequestURL = cut(c.RequestURL, 8192)
	c.Error = cut(c.Error, 4096)
	c.RequestBody = cut(c.RequestBody, CaptureBodyLimit+256)
	c.ResponseBody = cut(c.ResponseBody, CaptureBodyLimit+256)
	headers := func(src map[string][]string) map[string][]string {
		dst := make(map[string][]string)
		for k, values := range src {
			if len(dst) >= 32 || remaining == 0 {
				c.Truncated = true
				break
			}
			name := cut(k, 256)
			for i, value := range values {
				if i == 16 || remaining == 0 {
					c.Truncated = true
					break
				}
				dst[name] = append(dst[name], cut(value, 2048))
			}
		}
		return dst
	}
	c.RequestHeader = headers(c.RequestHeader)
	c.ResponseHeader = headers(c.ResponseHeader)
	events := make([]CaptureNetworkEvent, 0, min(len(c.NetworkEvents), 32))
	for i, event := range c.NetworkEvents {
		if i == 32 || remaining == 0 {
			c.Truncated = true
			break
		}
		event.Step = cut(event.Step, 128)
		event.Message = cut(event.Message, 1024)
		event.Error = cut(event.Error, 1024)
		events = append(events, event)
	}
	c.NetworkEvents = events
	c.Time = c.Time.UTC()
	return c
}

// PrepareCapture publishes the status and reserves completion order before a
// forwarding slot is released. Encoding later cannot overwrite a newer result.
func (s *Store) PrepareCapture(c Capture) Capture {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return c
	}
	key, exists := s.keys[c.KeyID]
	if !exists {
		return c
	}
	c.generation = s.keyGeneration[c.KeyID]
	c.revision = s.nextRevisionLocked()
	s.latestCaptureSequence[c.KeyID] = c.revision
	key.LastStatusCode = c.StatusCode
	s.keys[c.KeyID] = key
	s.dirtyKeys[c.KeyID] = c.revision
	return c
}

func (s *Store) enqueueCapture(c Capture) error {
	if c.revision == 0 {
		c = s.PrepareCapture(c)
	}
	if c.revision == 0 {
		return ErrNotFound
	}
	generation, sequence := c.generation, c.revision
	c = boundedCapture(c)
	raw, err := json.Marshal(c)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.keyGeneration[c.KeyID] != generation {
		return ErrNotFound
	}
	if s.latestCaptureSequence[c.KeyID] != sequence {
		return nil
	}
	// A slower encoder must not replace a newer completion for the same key.
	for _, existing := range []*queuedCapture{s.pendingCaptures[c.KeyID], s.flushingCaptures[c.KeyID]} {
		if existing != nil && existing.sequence > sequence {
			return nil
		}
	}
	if len(raw) > captureRecordLimit {
		s.droppedCaptures++
		return nil
	}
	s.dropPendingCaptureLocked(c.KeyID, false)
	for len(s.pendingCaptures)+len(s.flushingCaptures) >= captureQueueLimit || s.captureBytes+len(raw) > captureQueueBytes {
		oldest := ""
		var revision uint64
		for id, item := range s.pendingCaptures {
			if oldest == "" || item.sequence < revision {
				oldest = id
				revision = item.sequence
			}
		}
		if oldest == "" {
			s.droppedCaptures++
			return nil
		}
		s.dropPendingCaptureLocked(oldest, true)
	}
	s.pendingCaptures[c.KeyID] = &queuedCapture{raw: raw, sequence: sequence, generation: generation}
	s.captureBytes += len(raw)
	return nil
}

func (s *Store) dropPendingCaptureLocked(id string, overflow bool) {
	if item := s.pendingCaptures[id]; item != nil {
		delete(s.pendingCaptures, id)
		s.captureBytes -= len(item.raw)
		if overflow {
			s.droppedCaptures++
		}
	}
}

func (s *Store) persistCaptures() error {
	s.mu.Lock()
	batch := make(map[string]*queuedCapture)
	size := 0
	for id, item := range s.pendingCaptures {
		if size+len(item.raw) > captureBatchBytes {
			continue
		}
		delete(s.pendingCaptures, id)
		s.flushingCaptures[id] = item
		batch[id] = item
		size += len(item.raw)
	}
	s.mu.Unlock()
	if len(batch) == 0 {
		return nil
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(captureBucket))
		for id, item := range batch {
			if err := b.Put([]byte(id), item.raw); err != nil {
				return err
			}
		}
		return nil
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordPersistResultLocked(err)
	for id, item := range batch {
		delete(s.flushingCaptures, id)
		if err != nil && s.keyGeneration[id] == item.generation && s.pendingCaptures[id] == nil {
			s.pendingCaptures[id] = item // budget includes in-flight items; no growth on retry
		} else {
			s.captureBytes -= len(item.raw)
			if err == nil && s.keyGeneration[id] == item.generation {
				delete(s.ignoreStoredCapture, id)
			}
		}
	}
	return err
}
