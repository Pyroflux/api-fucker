package gateway

import (
	"strings"

	"api-fucker/internal/store"
)

// Protected by Gateway.mu. Store snapshots use a monotonic revision, so the
// steady request path has no list comparisons, copies, URL parsing, or I/O.
type manualProxyState struct {
	revision uint64
	source   string
	mode     string
	urls     []string
	err      error
	next     int
}

// Call with Gateway.mu already held. Key overrides and VPS selections do not
// consume the global manual proxy cursor, which is independent of the VPS cursor.
func (g *Gateway) manualProxyForKeyLocked(key store.Key, cfg store.GlobalConfig) (string, error) {
	if proxy := strings.TrimSpace(key.ProxyURL); proxy != "" {
		return proxy, nil
	}
	p := &g.manualProxies
	revision := cfg.RuntimeRevision()
	// Requests can reach this lock out of snapshot order. An old snapshot must
	// not roll back the active list/mode or reset its cursor. A request which has
	// not selected a proxy yet joins the currently active rotation instead.
	// Directly constructed configs have no revision and use content comparison.
	if revision == 0 || revision > p.revision {
		if p.source != cfg.DefaultProxyURL {
			p.urls, p.err = store.ParseDefaultProxyURLs(cfg.DefaultProxyURL)
			p.next = 0
		}
		mode := store.ProxyModeRoundRobin
		if cfg.ProxyMode == store.ProxyModeKeyBinding {
			mode = store.ProxyModeKeyBinding
		}
		if p.mode != mode {
			p.mode = mode
			p.next = 0
		}
		p.source = cfg.DefaultProxyURL
		p.revision = revision
	}
	if p.err != nil {
		return "", p.err
	}
	if len(p.urls) == 0 {
		return "", nil
	}
	if p.mode == store.ProxyModeKeyBinding {
		return p.urls[int(stableHash(key.ID)%uint32(len(p.urls)))], nil
	}
	proxy := p.urls[p.next]
	p.next = (p.next + 1) % len(p.urls)
	return proxy, nil
}
