package main

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type reloadRoundTripper func(*http.Request) (*http.Response, error)

func (f reloadRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type reloadResponseBody struct {
	io.Reader
	closed bool
}

func (b *reloadResponseBody) Close() error {
	b.closed = true
	return nil
}

func TestTriggerReloadRequestErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		url  string
	}{
		{name: "invalid URL", url: "://invalid"},
		{name: "transport error", url: "http://grafana.test/reload"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalConfig, originalTransport := cfg, http.DefaultTransport
			t.Cleanup(func() { cfg, http.DefaultTransport = originalConfig, originalTransport })
			cfg.RequestReloadURL = tc.url
			http.DefaultTransport = reloadRoundTripper(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("Grafana unavailable")
			})

			TriggerReload()
		})
	}
}

func TestTriggerReloadClosesResponse(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			originalConfig, originalTransport := cfg, http.DefaultTransport
			t.Cleanup(func() { cfg, http.DefaultTransport = originalConfig, originalTransport })
			cfg.RequestReloadURL = "http://grafana.test/reload"
			cfg.RequestUser, cfg.RequestPassword = "user", "password"
			body := &reloadResponseBody{Reader: strings.NewReader("{}")}
			http.DefaultTransport = reloadRoundTripper(func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPost {
					t.Errorf("method = %s, want POST", req.Method)
				}
				if user, password, ok := req.BasicAuth(); !ok || user != "user" || password != "password" {
					t.Error("reload request did not preserve basic authentication")
				}
				return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: body}, nil
			})

			TriggerReload()
			if !body.closed {
				t.Error("reload response body was not closed")
			}
		})
	}
}
