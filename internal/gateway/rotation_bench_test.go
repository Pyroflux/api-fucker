package gateway

import (
	"api-fucker/internal/store"
	"fmt"
	"testing"
	"time"
)

// Uses an isolated metadata-only database; no real captures or upstream traffic.
func BenchmarkKeyRotation(b *testing.B) {
	for _, size := range []int{100, 7300} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			st, err := store.Open(b.TempDir() + "/bench.db")
			if err != nil {
				b.Fatal(err)
			}
			defer st.Close()
			if err := st.SaveGlobalConfig(store.GlobalConfig{UpstreamBaseURL: "https://example.invalid", KeyMaxRequestsPerMinute: 1000000}); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < size; i++ {
				_, err := st.CreateKey(store.Key{ID: fmt.Sprintf("key-%05d", i), Name: "benchmark", APIKey: "test-placeholder", MaxConcurrency: 1000000})
				if err != nil {
					b.Fatal(err)
				}
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
