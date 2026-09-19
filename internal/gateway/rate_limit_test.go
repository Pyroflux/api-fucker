package gateway

import (
	"api-fucker/internal/store"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type failing429Body struct{}

func (failing429Body) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (failing429Body) Close() error             { return nil }

func Test429AcrossUpstreamPaths(t *testing.T) {
	for _, mode := range []string{"chat", "stream", "image", "models", "admin-test", "admin-models", "broken-body", "canceled-body"} {
		t.Run(mode, func(t *testing.T) {
			st, err := store.Open(t.TempDir() + "/data.db")
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			key, err := st.CreateKey(store.Key{Name: "cooldown", APIKey: "test-placeholder", BaseURL: "https://example.invalid"})
			if err != nil {
				t.Fatal(err)
			}
			gw := New(st, nil)
			calls := 0
			upstreamStatus := 429
			gw.newClient = func(string) (*http.Client, error) {
				return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls++
					resp := &http.Response{StatusCode: upstreamStatus, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":"quota balance exhausted"}`)), Request: r}
					if mode == "broken-body" && upstreamStatus == 429 {
						resp.Body = failing429Body{}
					}
					if mode == "canceled-body" && upstreamStatus == 429 {
						resp.Body = cancel429Body{}
					}
					return resp, nil
				})}, nil
			}
			cfg, _ := st.GlobalConfig()
			switch mode {
			case "admin-test":
				_, _ = gw.TestKey(context.Background(), key, cfg, "test", "hello")
			case "admin-models":
				_, _ = gw.ListModelsForKey(context.Background(), key, cfg)
			default:
				path := chatPath
				method := "POST"
				body := `{"model":"test"}`
				if mode == "stream" {
					body = `{"model":"test","stream":true}`
				}
				if mode == "image" {
					path = imageGenerationsPath
				}
				if mode == "models" {
					path = modelsPath
					method = "GET"
				}
				rec := httptest.NewRecorder()
				gw.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
				if mode != "broken-body" && mode != "canceled-body" && rec.Code != 429 {
					t.Fatalf("response %d", rec.Code)
				}
			}
			k, _ := st.GetKey(key.ID)
			if !k.IsRateLimited(time.Now()) || k.Consecutive429 != 1 || k.Paused || k.BalanceDepleted || k.ConsecutiveErrors != 0 {
				t.Fatalf("incorrect state: %+v", k)
			}
			if calls != 1 {
				t.Fatalf("429 retried: %d", calls)
			}
			if _, err := gw.acquireKey(); !errors.Is(err, errNoAvailableKey) {
				t.Fatalf("cooling key selected: %v", err)
			}
			// Explicit tests remain allowed, and do not extend the original cooldown.
			_, _ = gw.TestKey(context.Background(), key, cfg, "test", "hello")
			updated, _ := st.GetKey(key.ID)
			if calls != 2 || !updated.RateLimitedUntil.Equal(k.RateLimitedUntil) {
				t.Fatal("manual test blocked or extended deadline")
			}
			upstreamStatus = 200
			if _, err := gw.TestKey(context.Background(), key, cfg, "test", "hello"); err != nil {
				t.Fatal(err)
			}
			updated, _ = st.GetKey(key.ID)
			if !updated.RateLimitedUntil.Equal(k.RateLimitedUntil) {
				t.Fatal("successful test cleared cooldown")
			}
		})
	}
}

type cancel429Body struct{}

func (cancel429Body) Read([]byte) (int, error) { return 0, context.Canceled }
func (cancel429Body) Close() error             { return nil }

func Test429SequenceCancellationAndExpiry(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := store.GlobalConfig{UpstreamBaseURL: "https://example.invalid", Key429Threshold: 2, Key429CooldownMinutes: 10}
	if err := st.SaveGlobalConfig(cfg); err != nil {
		t.Fatal(err)
	}
	key, err := st.CreateKey(store.Key{Name: "cooldown", APIKey: "test-placeholder"})
	if err != nil {
		t.Fatal(err)
	}
	gw := New(st, nil)
	gw.recordUpstreamResult(key.ID, 429, []byte("429"), nil, 1, true)
	gw.recordUpstreamResult(key.ID, 0, nil, context.Canceled, 1, true)
	k, _ := st.GetKey(key.ID)
	if k.Consecutive429 != 1 {
		t.Fatal("cancel reset sequence")
	}
	gw.recordUpstreamResult(key.ID, 200, nil, nil, 1, false)
	k, _ = st.GetKey(key.ID)
	if k.Consecutive429 != 0 {
		t.Fatal("models success not counted")
	}
	// An expired stored timestamp requires no cleanup task to make a key routable.
	expired, err := st.CreateKey(store.Key{Name: "expired", APIKey: "test-expired", RateLimitedUntil: time.Now().Add(-time.Minute), Consecutive429: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PauseKeys([]string{key.ID}); err != nil {
		t.Fatal(err)
	}
	selected, err := gw.acquireKey()
	if err != nil {
		t.Fatal(err)
	}
	gw.releaseSelected(selected)
	if selected.key.ID != expired.ID {
		t.Fatal("expired cooldown was not selectable")
	}
}
