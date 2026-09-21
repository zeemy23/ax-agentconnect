// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
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
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	exitCode, err := runWithSignals(context.Background(), cfg, os.Stdin, os.Stdout, os.Stderr, signals)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ax-agentconnect: %v\n", err)
		os.Exit(1)
	}
	os.Exit(exitCode)
}

func parseFlags(args []string, stderr io.Writer) (Config, error) {
	var (
		cfg         Config
		command     stringList
		commandArgs stringList
	)
	fs := flag.NewFlagSet("ax-agentconnect", flag.ContinueOnError)
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
	fs.BoolVar(&cfg.InsecureInCluster, "insecure-in-cluster", false, "allow plaintext gRPC to non-loopback cluster addresses")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	cfg.Command = append([]string(nil), command...)
	cfg.Command = append(cfg.Command, commandArgs...)
	cfg.Command = append(cfg.Command, fs.Args()...)
	if cfg.Atespace == "" {
		return Config{}, fmt.Errorf("--atespace cannot be empty")
	}
	if cfg.Task == "" {
		return Config{}, fmt.Errorf("--task is required")
	}
	if len(cfg.Command) == 0 {
		return Config{}, fmt.Errorf("--command is required (repeat it for each token, or put tokens after --)")
	}
	if err := validateEndpoint("AX server", cfg.Server, cfg.InsecureInCluster); err != nil {
		return Config{}, err
	}
	if err := validateEndpoint("router", cfg.Router, cfg.InsecureInCluster); err != nil {
		return Config{}, err
	}
	if err := validateACPCWD(cfg.ACPCWD); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
