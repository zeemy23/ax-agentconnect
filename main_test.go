// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"flag"
	"google.golang.org/grpc/metadata"
	"io"
	"testing"
)

func TestCLIRejectsUnsafeConfiguration(t *testing.T) {
	for _, args := range [][]string{
		{"--task", "../other", "--", "agent"},
		{"--task", "ok", "--atespace", "space/actor", "--", "agent"},
		{"--task", "ok", "--state-dir", "relative", "--", "agent"},
		{"--task", "ok", "--state-dir", "", "--", "agent"},
		{"--task", "ok", "--process-timeout", "0", "--", "agent"},
		{"--task", "ok", "--shutdown-timeout", "-1s", "--", "agent"},
		{"--task", "ok", "--process-timeout", "25h", "--", "agent"},
	} {
		if _, err := parseFlags(append([]string{"--raw-stdio"}, args...), io.Discard); err == nil {
			t.Fatalf("accepted %q", args)
		}
	}
	if _, err := parseFlags([]string{"--task", "ok", "--", "agent"}, io.Discard); err == nil {
		t.Fatal("implicit raw mode accepted")
	}
	if _, err := parseFlags([]string{"--help"}, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("help: %v", err)
	}
	cfg, err := parseFlags([]string{"--task", "ok", "--raw-stdio", "--", "agent", "--flag", ""}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Command) != 3 || cfg.Command[2] != "" {
		t.Fatalf("argv not exact: %#v", cfg.Command)
	}
}
func TestActorMetadataReplacesInheritedRouting(t *testing.T) {
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("ate-target-actor", "untrusted/other", "x-test", "value"))
	md, _ := metadata.FromOutgoingContext(actorContext(ctx, "default/fixed"))
	if got := md.Get("ate-target-actor"); len(got) != 1 || got[0] != "default/fixed" {
		t.Fatalf("routing: %v", got)
	}
	if md.Get("x-test")[0] != "value" {
		t.Fatal("unrelated metadata dropped")
	}
}
func TestTaskIdentityMismatchIsRejected(t *testing.T) {
	ax, _, address := simulatedServer(t, simulatedDuplex, nil)
	cfg := simulatedConfig(address)
	ax.task.Metadata.Name = "another"
	if _, err := checkTask(ax.task, cfg); err == nil {
		t.Fatal("wrong task accepted")
	}
	ax.task.Metadata.Name = cfg.Task
	cfg.ExpectedTaskID = "expected"
	if _, err := checkTask(ax.task, cfg); err == nil {
		t.Fatal("wrong task id accepted")
	}
}
