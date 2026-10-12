package store

import (
	"fmt"
	"strings"
)

// ParseDefaultProxyURLs accepts both legacy single URLs and newline-separated
// lists. Keep input line numbers for errors, and never include credentials in them.
func ParseDefaultProxyURLs(value string) ([]string, error) {
	var proxies []string
	seen := make(map[string]bool)
	for i, line := range strings.Split(value, "\n") {
		proxy := strings.TrimSpace(line)
		if proxy == "" {
			continue
		}
		if err := validateURL(proxy, "proxy URL"); err != nil {
			return nil, fmt.Errorf("default proxy URL on line %d: %w", i+1, err)
		}
		if !seen[proxy] {
			seen[proxy] = true
			proxies = append(proxies, proxy)
		}
	}
	return proxies, nil
}
