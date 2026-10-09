package gateway

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"api-fucker/internal/store"
)

func TestStreamingCaptureBoundDoesNotTruncateForwarding(t *testing.T) {
	body := strings.Repeat("data: token\n\n", 50000)
	resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
	output := httptest.NewRecorder()
	status, _, capture, err := streamResponse(output, resp)
	if err != nil || status != http.StatusOK || output.Body.String() != body {
		t.Fatalf("forwarding changed: status=%d bytes=%d err=%v", status, output.Body.Len(), err)
	}
	if len(capture) > store.CaptureBodyLimit+len(store.CaptureTruncationMarker) || !bytes.HasSuffix(capture, []byte(store.CaptureTruncationMarker)) {
		t.Fatalf("unbounded diagnostic: %d bytes", len(capture))
	}
}
