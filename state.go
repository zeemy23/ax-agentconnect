// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ponytail: launchGuard is a local single-owner interlock. Multi-host admission
// needs a durable controller and upstream fencing/idempotency support. Uncertain
// starts deliberately leave the receipt.
type launchGuard struct {
	file   *os.File
	path   string
	record launchRecord
}

type launchRecord struct {
	Version     int       `json:"version"`
	StartedAt   time.Time `json:"started_at"`
	Server      string    `json:"server"`
	Router      string    `json:"router"`
	Task        string    `json:"task"`
	Atespace    string    `json:"atespace"`
	Actor       string    `json:"actor"`
	TaskID      string    `json:"task_id,omitempty"`
	CommandHash string    `json:"command_sha256"`
	ProcessID   string    `json:"process_id,omitempty"`
}

func acquireLaunch(cfg Config, actor, taskID string) (*launchGuard, error) {
	if !filepath.IsAbs(cfg.StateDir) {
		return nil, errors.New("state directory must be absolute")
	}
	if err := os.MkdirAll(cfg.StateDir, 0700); err != nil {
		return nil, fmt.Errorf("creating launch state: %w", err)
	}
	info, err := os.Lstat(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("state directory must be a private directory (mode 0700), not a symlink")
	}
	// Persist every newly created directory entry, not only the leaf. A durable
	// receipt is useless if a first-launch crash loses its ancestor directory.
	for directory := cfg.StateDir; ; directory = filepath.Dir(directory) {
		if err := syncDirectory(directory); err != nil {
			return nil, fmt.Errorf("syncing launch state directory: %w", err)
		}
		if filepath.Dir(directory) == directory {
			break
		}
	}
	// Configured AX authority must not have DNS aliases or separate state roots
	// across launchers. Changing the router or argv must not evade this guard.
	endpoint, err := parseEndpoint(cfg.Server)
	if err != nil {
		return nil, err
	}
	authority := strings.ToLower(endpoint.address)
	key, _ := json.Marshal([]string{authority, cfg.Atespace, cfg.Task})
	sum := sha256.Sum256(key)
	path := filepath.Join(cfg.StateDir, hex.EncodeToString(sum[:])+".json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("launch blocked by existing receipt %s; reconcile the recorded remote process before removing it; do not automatically retry", path)
	}
	if err != nil {
		return nil, fmt.Errorf("creating launch receipt: %w", err)
	}
	command, _ := json.Marshal(cfg.Command)
	commandHash := sha256.Sum256(command)
	guard := &launchGuard{file: file, path: path, record: launchRecord{Version: 1, StartedAt: time.Now().UTC(), Server: cfg.Server, Router: cfg.Router, Task: cfg.Task, Atespace: cfg.Atespace, Actor: actor, TaskID: taskID, CommandHash: hex.EncodeToString(commandHash[:])}}
	if err := guard.save(); err != nil {
		return nil, guard.abortBeforeStart(err)
	}
	if err := syncDirectory(cfg.StateDir); err != nil {
		return nil, guard.abortBeforeStart(err)
	}
	return guard, nil
}

func (g *launchGuard) save() error {
	data, err := json.MarshalIndent(g.record, "", "  ")
	if err != nil {
		return err
	}
	if _, err = g.file.Seek(0, 0); err != nil {
		return err
	}
	if err = g.file.Truncate(0); err != nil {
		return err
	}
	if _, err = g.file.Write(append(data, '\n')); err != nil {
		return err
	}
	return g.file.Sync()
}
func (g *launchGuard) started(id string) error { g.record.ProcessID = id; return g.save() }
func (g *launchGuard) close()                  { _ = g.file.Close() }
func (g *launchGuard) complete() error {
	if err := g.file.Close(); err != nil {
		return err
	}
	if err := os.Remove(g.path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(g.path))
}
func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// Called only before StartProcess can be issued. There is no ambiguous remote
// side effect at this point, so remove an unsuccessfully persisted new receipt.
func (g *launchGuard) abortBeforeStart(cause error) error {
	closeErr := g.file.Close()
	removeErr := os.Remove(g.path)
	syncErr := syncDirectory(filepath.Dir(g.path))
	return errors.Join(cause, closeErr, removeErr, syncErr)
}
