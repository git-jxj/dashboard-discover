package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

func TestConfigMapDiscoverReload(t *testing.T) {
	const oldData = `{"version":1}`
	const newData = `{"version":2}`
	for _, event := range []string{"add", "update"} {
		for _, tc := range []struct {
			name       string
			hardlinks  bool
			data       string
			reload     bool
			wantReload int32
		}{
			{"mixed results", true, newData, true, 1},
			{"all changed", false, newData, true, 1},
			{"unchanged", false, oldData, true, 0},
			{"reload disabled", false, newData, false, 0},
		} {
			t.Run(event+"/"+tc.name, func(t *testing.T) {
				originalConfig := cfg
				t.Cleanup(func() { cfg = originalConfig })
				cfg = Config{
					Folder:               t.TempDir(),
					NamespaceAsSubFolder: true,
					ConfigMapSelector:    "dashboard=true",
					EnableReload:         tc.reload,
				}
				core, logs := observer.New(zapcore.DebugLevel)
				originalLogger := zap.L()
				zap.ReplaceGlobals(zap.New(core))
				t.Cleanup(func() { zap.ReplaceGlobals(originalLogger) })

				keys := []string{"one.json", "two.json", "three.json"}
				files := make([]string, len(keys))
				for i, key := range keys {
					files[i] = BuildPath(BuildFileName(key, "test", "cm"), "test")
					if err := os.MkdirAll(filepath.Dir(files[i]), 0755); err != nil {
						t.Fatal(err)
					}
					if tc.hardlinks && i > 0 {
						// The first write changes the shared inode; every later upsert
						// is unchanged, regardless of Go's map iteration order.
						if err := os.Link(files[0], files[i]); err != nil {
							t.Fatal(err)
						}
					} else if err := os.WriteFile(files[i], []byte(oldData), 0644); err != nil {
						t.Fatal(err)
					}
				}

				var reloads atomic.Int32
				reloadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPost {
						t.Errorf("reload method = %s, want POST", r.Method)
					}
					reloads.Add(1)
					w.WriteHeader(http.StatusOK)
				}))
				t.Cleanup(reloadServer.Close)
				cfg.RequestReloadURL = reloadServer.URL

				cm := &corev1.ConfigMap{
					TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
					ObjectMeta: metav1.ObjectMeta{
						Name: "dashboards", Namespace: "test", ResourceVersion: "1",
						Labels: map[string]string{"dashboard": "true"},
					},
					Data: make(map[string]string),
				}
				for _, key := range keys {
					cm.Data[key] = tc.data
					if event == "update" {
						cm.Data[key] = oldData
					}
				}
				events := make(chan watch.Event, 1)
				apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/api/v1/configmaps" {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					encoder := json.NewEncoder(w)
					if r.URL.Query().Get("watch") == "true" {
						flusher := w.(http.Flusher)
						w.WriteHeader(http.StatusOK)
						flusher.Flush()
						for {
							select {
							case next := <-events:
								if err := encoder.Encode(map[string]interface{}{"type": next.Type, "object": next.Object}); err != nil {
									return
								}
								flusher.Flush()
							case <-r.Context().Done():
								return
							}
						}
					}
					if err := encoder.Encode(corev1.ConfigMapList{
						TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMapList"},
						ListMeta: metav1.ListMeta{ResourceVersion: "1"},
						Items:    []corev1.ConfigMap{*cm},
					}); err != nil {
						t.Errorf("encode ConfigMap list: %v", err)
					}
				}))
				t.Cleanup(apiServer.Close)

				controller, err := NewConfigMapDiscover(&rest.Config{
					Host:          apiServer.URL,
					ContentConfig: rest.ContentConfig{ContentType: "application/json"},
				})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() {
					defer close(done)
					controller.RunWithContext(ctx)
				}()
				t.Cleanup(func() {
					cancel()
					<-done
				})
				waitForEvent := func(action string) {
					t.Helper()
					message := fmt.Sprintf("configMap source dashboard %s: dashboards test", action)
					deadline := time.NewTimer(5 * time.Second)
					defer deadline.Stop()
					tick := time.NewTicker(time.Millisecond)
					defer tick.Stop()
					for logs.FilterMessage(message).Len() == 0 {
						select {
						case <-deadline.C:
							t.Fatalf("did not finish %s handler", action)
						case <-tick.C:
						}
					}
				}
				waitForEvent("added")
				if event == "update" {
					if got := reloads.Load(); got != 0 {
						t.Fatalf("unchanged initial list caused %d reloads", got)
					}
					updated := cm.DeepCopy()
					updated.ResourceVersion = "2"
					for _, key := range keys {
						updated.Data[key] = tc.data
					}
					events <- watch.Event{Type: watch.Modified, Object: updated}
					waitForEvent("updated")
				}
				if got := reloads.Load(); got != tc.wantReload {
					t.Errorf("reload requests = %d, want %d", got, tc.wantReload)
				}
				for _, file := range files {
					data, err := os.ReadFile(file)
					if err != nil {
						t.Fatal(err)
					}
					if string(data) != tc.data {
						t.Errorf("file %s = %s, want %s", file, data, tc.data)
					}
				}
			})
		}
	}
}
