// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

type stringList []string

func (s *stringList) String() string { return fmt.Sprint([]string(*s)) }

func (s *stringList) Set(value string) error {
	if value == "" {
		return fmt.Errorf("empty command argument")
	}
	*s = append(*s, value)
	return nil
}

func main() {
	cfg, err := parseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	exitCode, err := runWithSignals(context.Background(), cfg, os.Stdin, os.Stdout, os.Stderr, signals)
	if err != nil {
		cliDiagnostic(fmt.Sprintf("ax-agentconnect: %v\n", err))
		os.Exit(1)
	}
	os.Exit(exitCode)
}

func parseFlags(args []string, stderr io.Writer) (Config, error) {
	var (
		cfg         Config
		command     stringList
		commandArgs stringList
		rawStdio    bool
	)
	home, homeErr := os.UserHomeDir()
	if homeErr != nil {
		return Config{}, fmt.Errorf("locating state directory: %w", homeErr)
	}
	fs := flag.NewFlagSet("ax-agentconnect", flag.ContinueOnError)
	fs.BoolVar(&rawStdio, "raw-stdio", false, "explicit trusted raw byte transport; disables ACP validation and capability filtering")
	fs.SetOutput(stderr)
	fs.StringVar(&cfg.Task, "task", "", "fixed AX task name")
	fs.StringVar(&cfg.Atespace, "atespace", "default", "AX atespace containing the task")
	fs.StringVar(&cfg.Atespace, "a", "default", "alias for --atespace")
	fs.StringVar(&cfg.Server, "server", "127.0.0.1:8080", "AX gRPC server address")
	fs.StringVar(&cfg.Router, "router", "127.0.0.1:8001", "atenet router gRPC address")
	fs.Var(&command, "command", "one command token; repeat or use tokens after --")
	fs.Var(&commandArgs, "arg", "one command argument token; repeat as needed")
	fs.Var(&commandArgs, "command-arg", "alias for --arg")
	fs.StringVar(&cfg.ACPCWD, "acp-cwd", "", "operator-fixed absolute cwd for ACP session/new and session/load")
	fs.StringVar(&cfg.StateDir, "state-dir", filepath.Join(home, ".local", "state", "ax-agentconnect"), "persistent private local launch receipt directory; share between all launchers for a task")
	fs.StringVar(&cfg.ExpectedTaskID, "expected-task-id", "", "require AX status.id to match (preflight check, not atomic fencing)")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "grace period after EOF or shutdown signal")
	fs.DurationVar(&cfg.ProcessTimeout, "process-timeout", time.Hour, "guest-enforced maximum process lifetime")
	for _, endpoint := range []struct {
		name   string
		config *transportConfig
	}{{"server", &cfg.ServerTLS}, {"router", &cfg.RouterTLS}} {
		fs.StringVar(&endpoint.config.CAFile, endpoint.name+"-ca", "", "PEM CA bundle for HTTPS endpoint (default system roots)")
		fs.StringVar(&endpoint.config.CertFile, endpoint.name+"-cert", "", "PEM client certificate for mTLS")
		fs.StringVar(&endpoint.config.KeyFile, endpoint.name+"-key", "", "PEM client key for mTLS")
		fs.StringVar(&endpoint.config.ServerName, endpoint.name+"-tls-name", "", "TLS server certificate name override")
	}
	fs.BoolVar(&cfg.InsecureInCluster, "insecure-in-cluster", false, "allow plaintext gRPC to non-loopback cluster addresses")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	cfg.Command = append([]string(nil), command...)
	cfg.Command = append(cfg.Command, commandArgs...)
	cfg.Command = append(cfg.Command, fs.Args()...)
	if rawStdio && cfg.ACPCWD != "" {
		return Config{}, fmt.Errorf("--raw-stdio and --acp-cwd cannot be combined")
	}
	if !rawStdio && cfg.ACPCWD == "" {
		return Config{}, fmt.Errorf("--acp-cwd is required for ACP; use --raw-stdio only for trusted byte transport")
	}
	if cfg.ShutdownTimeout <= 0 || cfg.ProcessTimeout <= 0 {
		return Config{}, fmt.Errorf("timeouts must be positive")
	}
	if cfg.StateDir == "" || !filepath.IsAbs(cfg.StateDir) {
		return Config{}, fmt.Errorf("--state-dir must be an absolute persistent directory")
	}
	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Diagnostics must not defeat the shutdown deadline when the parent's stderr
// pipe is blocked. The CLI exits immediately afterward; this is not a reusable
// asynchronous logger.
func cliDiagnostic(message string) {
	done := make(chan struct{})
	go func() { _, _ = io.WriteString(os.Stderr, message); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
	}
}
