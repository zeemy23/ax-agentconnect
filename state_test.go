// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLaunchGuardPersistsUncertaintyAndExcludesCommand(t *testing.T) {
	cfg := simulatedConfig("127.0.0.1:12345")
	cfg.StateDir = filepath.Join(t.TempDir(), "state")
	cfg.Command = []string{"agent", "secret-value"}
	guard, err := acquireLaunch(cfg, "actor", "task-id")
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.started("process-123"); err != nil {
		t.Fatal(err)
	}
	guard.close()
	raw, err := os.ReadFile(guard.path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret-value") {
		t.Fatal("command leaked")
	}
	var record launchRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		t.Fatal(err)
	}
	if record.ProcessID != "process-123" || record.TaskID != "task-id" {
		t.Fatalf("receipt: %+v", record)
	}
	if _, err := acquireLaunch(cfg, "replacement-actor", "replacement-id"); err == nil {
		t.Fatal("uncertain launch retried")
	}
}
func TestLaunchGuardReleasesOnlyAfterCompletion(t *testing.T) {
	cfg := simulatedConfig("127.0.0.1:12345")
	cfg.StateDir = filepath.Join(t.TempDir(), "state")
	guard, err := acquireLaunch(cfg, "actor", "id")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireLaunch(cfg, "actor", "id"); err == nil {
		t.Fatal("concurrent launch accepted")
	}
	if err := guard.complete(); err != nil {
		t.Fatal(err)
	}
	second, err := acquireLaunch(cfg, "actor", "id")
	if err != nil {
		t.Fatal(err)
	}
	second.close()
}
func TestLaunchGuardRejectsSharedOrSymlinkDirectory(t *testing.T) {
	cfg := simulatedConfig("127.0.0.1:12345")
	cfg.StateDir = filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(cfg.StateDir, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireLaunch(cfg, "actor", "id"); err == nil {
		t.Fatal("shared state accepted")
	}
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	cfg.StateDir = link
	if _, err := acquireLaunch(cfg, "actor", "id"); err == nil {
		t.Fatal("symlink accepted")
	}
}

func TestSimulatedAmbiguousStartBlocksNewInvocation(t *testing.T) {
	_, process, address := simulatedServer(t, simulatedStartError, errors.New("transport lost"))
	cfg := simulatedConfig(address)
	cfg.StateDir = filepath.Join(t.TempDir(), "state")
	for i := 0; i < 2; i++ {
		if _, err := runWithSignals(context.Background(), cfg, strings.NewReader(""), io.Discard, io.Discard, nil); err == nil {
			t.Fatal("ambiguous start succeeded")
		}
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.startCalls != 1 {
		t.Fatalf("remote starts=%d, want one across invocations", process.startCalls)
	}
}
func TestSimulatedConfirmedExitReleasesReceipt(t *testing.T) {
	_, _, address := simulatedServer(t, simulatedDuplex, nil)
	cfg := simulatedConfig(address)
	cfg.StateDir = filepath.Join(t.TempDir(), "state")
	code, err := runWithSignals(context.Background(), cfg, strings.NewReader("test"), io.Discard, io.Discard, nil)
	if err != nil || code != 7 {
		t.Fatalf("exit=%d err=%v", code, err)
	}
	files, err := os.ReadDir(cfg.StateDir)
	if err != nil || len(files) != 0 {
		t.Fatalf("receipts remain: %v, %v", files, err)
	}
}
