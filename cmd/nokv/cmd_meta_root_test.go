package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func threePeerArgs() []string {
	return []string{
		"-peer", "1=127.0.0.1:7001",
		"-peer", "2=127.0.0.1:7002",
		"-peer", "3=127.0.0.1:7003",
	}
}

func TestRunMetaRootCmdStartsAndStops(t *testing.T) {
	origNotify := metaRootNotifyContext
	metaRootNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		cancel()
		return ctx, cancel
	}
	t.Cleanup(func() { metaRootNotifyContext = origNotify })

	var buf bytes.Buffer
	args := append([]string{
		"-addr", "127.0.0.1:0",
		"-workdir", t.TempDir(),
		"-node-id", "1",
		"-transport-addr", "127.0.0.1:0",
	}, threePeerArgs()...)
	if err := runMetaRootCmd(&buf, args); err != nil {
		t.Fatalf("runMetaRootCmd: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "Metadata root service listening on") {
		t.Fatalf("expected listen line, got %q", out)
	}
	if !strings.Contains(out, "Metadata root node id: 1") {
		t.Fatalf("expected node id line, got %q", out)
	}
}

func TestRunMetaRootCmdRequiresWorkdir(t *testing.T) {
	var buf bytes.Buffer
	args := append([]string{
		"-addr", "127.0.0.1:0",
		"-node-id", "1",
		"-transport-addr", "127.0.0.1:0",
	}, threePeerArgs()...)
	err := runMetaRootCmd(&buf, args)
	if err == nil || !strings.Contains(err.Error(), "--workdir is required") {
		t.Fatalf("expected --workdir error, got %v", err)
	}
}

func TestRunMetaRootCmdRequiresNodeID(t *testing.T) {
	var buf bytes.Buffer
	args := append([]string{
		"-addr", "127.0.0.1:0",
		"-workdir", t.TempDir(),
		"-transport-addr", "127.0.0.1:0",
	}, threePeerArgs()...)
	err := runMetaRootCmd(&buf, args)
	if err == nil || !strings.Contains(err.Error(), "--node-id is required") {
		t.Fatalf("expected --node-id error, got %v", err)
	}
}

func TestRunMetaRootCmdRequiresTransportAddr(t *testing.T) {
	var buf bytes.Buffer
	args := append([]string{
		"-addr", "127.0.0.1:0",
		"-workdir", t.TempDir(),
		"-node-id", "1",
	}, threePeerArgs()...)
	err := runMetaRootCmd(&buf, args)
	if err == nil || !strings.Contains(err.Error(), "--transport-addr is required") {
		t.Fatalf("expected --transport-addr error, got %v", err)
	}
}

func TestRunMetaRootCmdRequiresThreePeers(t *testing.T) {
	var buf bytes.Buffer
	err := runMetaRootCmd(&buf, []string{
		"-addr", "127.0.0.1:0",
		"-workdir", t.TempDir(),
		"-node-id", "1",
		"-transport-addr", "127.0.0.1:0",
		"-peer", "1=127.0.0.1:7001",
		"-peer", "2=127.0.0.1:7002",
	})
	if err == nil || !strings.Contains(err.Error(), "requires exactly 3 --peer") {
		t.Fatalf("expected 3-peer error, got %v", err)
	}
}

// TestRunMetaRootCmdExposesMetricsEndpoint verifies that --metrics-addr starts
// an expvar HTTP endpoint and publishes the nokv_meta_root variable with at
// least the addr / node_id / last_committed fields.
func TestRunMetaRootCmdExposesMetricsEndpoint(t *testing.T) {
	origNotify := metaRootNotifyContext
	metricsReady := make(chan string, 1)
	cancelReady := make(chan struct{})
	var once sync.Once

	metaRootNotifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(parent)
		go func() {
			select {
			case <-cancelReady:
				cancel()
			case <-time.After(5 * time.Second):
				cancel()
			}
		}()
		return ctx, cancel
	}
	t.Cleanup(func() { metaRootNotifyContext = origNotify })

	var buf bytes.Buffer
	var runErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		args := append([]string{
			"-addr", "127.0.0.1:0",
			"-workdir", t.TempDir(),
			"-node-id", "1",
			"-transport-addr", "127.0.0.1:0",
			"-metrics-addr", "127.0.0.1:0",
		}, threePeerArgs()...)
		runErr = runMetaRootCmd(&buf, args)
	}()

	deadline := time.Now().Add(5 * time.Second)
	var metricsURL string
	for time.Now().Before(deadline) {
		if url := extractMetricsURL(buf.String()); url != "" {
			metricsURL = url
			once.Do(func() { metricsReady <- url })
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if metricsURL == "" {
		close(cancelReady)
		<-done
		t.Fatalf("metrics endpoint never started; output=%q", buf.String())
	}

	resp, err := http.Get(metricsURL)
	if err != nil {
		close(cancelReady)
		<-done
		t.Fatalf("GET %s: %v", metricsURL, err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		close(cancelReady)
		<-done
		t.Fatalf("parse /debug/vars: %v  body=%s", err, string(body))
	}

	raw, ok := payload["nokv_meta_root"]
	if !ok {
		close(cancelReady)
		<-done
		t.Fatalf("nokv_meta_root missing; payload keys=%v", mapKeys(payload))
	}
	entry, ok := raw.(map[string]any)
	if !ok {
		close(cancelReady)
		<-done
		t.Fatalf("nokv_meta_root not an object; got %T", raw)
	}
	if _, has := entry["addr"]; !has {
		close(cancelReady)
		<-done
		t.Fatalf("expected addr field in nokv_meta_root; got %v", entry)
	}
	if _, has := entry["node_id"]; !has {
		close(cancelReady)
		<-done
		t.Fatalf("expected node_id field in nokv_meta_root; got %v", entry)
	}
	if _, has := entry["last_committed"]; !has {
		close(cancelReady)
		<-done
		t.Fatalf("expected last_committed field from Snapshot(); got %v", entry)
	}

	close(cancelReady)
	<-done
	if runErr != nil {
		t.Fatalf("runMetaRootCmd returned error: %v", runErr)
	}
}

func extractMetricsURL(out string) string {
	const marker = "metrics endpoint listening on "
	_, after, ok := strings.Cut(out, marker)
	if !ok {
		return ""
	}
	tail := after
	before, _, ok := strings.Cut(tail, "\n")
	if !ok {
		return strings.TrimSpace(tail)
	}
	return strings.TrimSpace(before)
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
