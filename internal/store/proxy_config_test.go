package store

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestGlobalConfigRuntimeRevision(t *testing.T) {
	st, err := Open(t.TempDir() + "/data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	initial, _ := st.GlobalConfig()
	if initial.RuntimeRevision() == 0 {
		t.Fatal("initial configuration has no runtime revision")
	}
	cfg := initial
	cfg.UpstreamBaseURL = "https://upstream.example"
	for i := 0; i < 3; i++ {
		previous := cfg.RuntimeRevision()
		cfg.GlobalMaxConcurrency = i
		if err := st.SaveGlobalConfig(cfg); err != nil {
			t.Fatal(err)
		}
		cfg, _ = st.GlobalConfig()
		if cfg.RuntimeRevision() <= previous {
			t.Fatal("configuration revision did not increase")
		}
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(raw)), "revision") {
		t.Fatal("runtime revision leaked into config JSON")
	}
	var decoded GlobalConfig
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.RuntimeRevision() != 0 {
		t.Fatal("runtime revision survived serialization")
	}
	// Old caller snapshots cannot assign their revision to a newly saved config.
	if err := st.SaveGlobalConfig(decoded); err != nil {
		t.Fatal(err)
	}
	latest, _ := st.GlobalConfig()
	if latest.RuntimeRevision() <= cfg.RuntimeRevision() {
		t.Fatal("saving an unversioned API config reset the revision")
	}
}

func TestParseDefaultProxyURLs(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		want        []string
	}{
		{name: "empty", input: " \r\n\t\n"},
		{name: "legacy", input: "http://proxy.example:8080", want: []string{"http://proxy.example:8080"}},
		{name: "trim deduplicate preserve order", input: "\r\n http://a.example:8080 \r\n\nhttps://b.example:8443\r\nhttp://a.example:8080\n", want: []string{"http://a.example:8080", "https://b.example:8443"}},
		{name: "authentication and IPv6", input: "http://test-user:test-password@[::1]:8080\nhttps://b.example:8443", want: []string{"http://test-user:test-password@[::1]:8080", "https://b.example:8443"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseDefaultProxyURLs(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatal("parsed addresses differ from expected normalized list")
			}
		})
	}
	for _, bad := range []string{"proxy.example:8080", "socks5://proxy.example:1080", "http://test-user:test-password@proxy.example/%invalid"} {
		got, err := ParseDefaultProxyURLs("http://a.example:8080\n\n" + bad)
		if err == nil || !strings.Contains(err.Error(), "line 3") {
			t.Fatalf("missing original line number: %v", err)
		}
		if got != nil {
			t.Fatal("invalid list returned partial configuration")
		}
		if strings.Contains(err.Error(), "test-user") || strings.Contains(err.Error(), "test-password") {
			t.Fatal("validation exposed proxy credentials")
		}
	}
}

func TestDefaultProxyListPersistenceAndInvalidSave(t *testing.T) {
	path := t.TempDir() + "/data.db"
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	for _, tc := range []struct{ input, want string }{
		{"http://legacy.example:8080", "http://legacy.example:8080"},
		{"\n http://a.example:8080\r\nhttps://b.example:8443\nhttp://a.example:8080\n", "http://a.example:8080\nhttps://b.example:8443"},
		{" \n\r\n", ""},
	} {
		cfg := GlobalConfig{UpstreamBaseURL: "https://upstream.example", DefaultProxyURL: tc.input, ProxyMode: ProxyModeKeyBinding}
		if err := st.SaveGlobalConfig(cfg); err != nil {
			t.Fatal(err)
		}
		saved, err := st.GlobalConfig()
		if err != nil {
			t.Fatal(err)
		}
		if saved.DefaultProxyURL != tc.want || saved.ProxyMode != ProxyModeKeyBinding {
			t.Fatal("configuration not normalized")
		}
		invalid := saved
		invalid.DefaultProxyURL = "http://good.example:8080\ninvalid"
		invalid.GlobalMaxConcurrency = 99
		if err := st.SaveGlobalConfig(invalid); err == nil {
			t.Fatal("invalid proxy configuration was accepted")
		}
		unchanged, _ := st.GlobalConfig()
		if !reflect.DeepEqual(unchanged, saved) {
			t.Fatal("failed save partially changed live configuration")
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		st, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		reopened, _ := st.GlobalConfig()
		if reopened.RuntimeRevision() == 0 {
			t.Fatal("reopened config has no runtime revision")
		}
		// Runtime revisions belong to a Store instance, not the persisted config.
		saved.revision = reopened.revision
		if !reflect.DeepEqual(reopened, saved) {
			t.Fatal("configuration changed across restart")
		}
	}
}
