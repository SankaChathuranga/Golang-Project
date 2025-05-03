package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

type RPC string // RPC identifies the Raft method.

const (
	RPCRequestVote   RPC = "request_vote"
	RPCAppendEntries RPC = "append_entries"
)

var (
	ErrInvalidResponse = errors.New("invalid response from server")
	ErrTimeout         = errors.New("request timed out")
)

// HandlerFunc handles an inbound RPC and returns response or error.
type HandlerFunc func(method RPC, body io.Reader, w http.ResponseWriter)

// HTTPTransport routes JSON‑encoded RPCs over http.Client.
type HTTPTransport struct {
	client  *http.Client
	handler HandlerFunc
}

func New(handler HandlerFunc) *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{
			Timeout: 3 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
				MaxConnsPerHost:     100,
				MaxIdleConnsPerHost: 100,
			},
		},
		handler: handler,
	}
}

func (t *HTTPTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.handler(RPC(r.URL.Path[1:]), r.Body, w)
}

func (t *HTTPTransport) Call(addr string, method RPC, req, resp any) error {
	buf, err := json.Marshal(req)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		"http://"+addr+"/"+string(method),
		bytes.NewReader(buf))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrTimeout
		}
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return ErrInvalidResponse
	}

	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		return err
	}

	return nil
}

// Utility to reply JSON.
func ReplyJSON(w http.ResponseWriter, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(data); err != nil {
		// Log error but can't do much more since headers are already sent
		return
	}
}
