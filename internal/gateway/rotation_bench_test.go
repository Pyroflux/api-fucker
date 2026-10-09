package gateway

import (
	"api-fucker/internal/store"
	"encoding/json"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"testing"
	"time"
)

// Uses an isolated metadata-only database; no real captures or upstream traffic.
func BenchmarkKeyRotation(b *testing.B) {
	for _, size := range []int{100, 7300} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			path := b.TempDir() + "/bench.db"
			st, err := store.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			if err := st.Close(); err != nil {
				b.Fatal(err)
			}
			db, err := bolt.Open(path, 0600, nil)
			if err != nil {
				b.Fatal(err)
			}
			err = db.Update(func(tx *bolt.Tx) error {
				for i := 0; i < size; i++ {
					id := fmt.Sprintf("key-%05d", i)
					raw, err := json.Marshal(store.Key{ID: id, Name: "benchmark", APIKey: "test-placeholder", Enabled: true, MaxConcurrency: 1000000})
					if err != nil {
						return err
					}
					if err := tx.Bucket([]byte("keys")).Put([]byte(id), raw); err != nil {
						return err
					}
				}
				now := time.Now().UTC().Truncate(time.Minute)
				for i := 0; i < 7*24*60; i++ {
					at := now.Add(-time.Duration(i) * time.Minute)
					raw, err := json.Marshal(map[string]any{"time": at, "requests": 1})
					if err != nil {
						return err
					}
					if err := tx.Bucket([]byte("metrics")).Put([]byte(fmt.Sprintf("%020d", at.Unix())), raw); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			st, err = store.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()
			if err := st.SaveGlobalConfig(store.GlobalConfig{UpstreamBaseURL: "https://example.invalid", KeyMaxRequestsPerMinute: 1000000}); err != nil {
				b.Fatal(err)
			}
			for _, mode := range []string{"available", "mostly_busy", "parallel"} {
				b.Run(mode, func(b *testing.B) {
					gw := New(st, nil)
					now := time.Now()
					for i := 0; i < size; i++ {
						id := fmt.Sprintf("key-%05d", i)
						gw.keyMinute[id] = &minuteCounter{hits: []time.Time{now}}
						if mode == "mostly_busy" && i < size-1 {
							gw.inflight[id] = 1000000
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					run := func() {
						k, err := gw.acquireKey()
						if err != nil {
							b.Error(err)
							return
						}
						gw.releaseSelected(k)
					}
					if mode == "parallel" {
						b.RunParallel(func(pb *testing.PB) {
							for pb.Next() {
								run()
							}
						})
					} else {
						for i := 0; i < b.N; i++ {
							run()
						}
					}
				})
			}
		})
	}
}
