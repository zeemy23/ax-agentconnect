// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
)

type outputStreamFixture struct {
	messages []*ateenvv1alpha.ProcessOutput
	errors   []error
	index    int
}

func (s *outputStreamFixture) Recv() (*ateenvv1alpha.ProcessOutput, error) {
	if s.index < len(s.errors) && s.errors[s.index] != nil {
		err := s.errors[s.index]
		s.index++
		return nil, err
	}
	if s.index >= len(s.messages) {
		return nil, io.EOF
	}
	message := s.messages[s.index]
	s.index++
	return message, nil
}

func exitedOutput(id string, code int32) *ateenvv1alpha.ProcessOutput {
	return &ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Exit{Exit: &ateenvv1alpha.Process{
		ProcessId: id,
		State:     ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED,
		ExitCode:  code,
	}}}
}

func TestReceiveOutputValidatesExitIdentityStateAndCode(t *testing.T) {
	tests := []struct {
		name string
		exit *ateenvv1alpha.Process
		want string
	}{
		{name: "wrong id", exit: &ateenvv1alpha.Process{ProcessId: "other", State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED}, want: "does not match"},
		{name: "wrong state", exit: &ateenvv1alpha.Process{ProcessId: "p", State: ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING}, want: "state"},
		{name: "negative code", exit: &ateenvv1alpha.Process{ProcessId: "p", State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, ExitCode: -1}, want: "invalid exit code"},
		{name: "large code", exit: &ateenvv1alpha.Process{ProcessId: "p", State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, ExitCode: 256}, want: "invalid exit code"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stream := &outputStreamFixture{messages: []*ateenvv1alpha.ProcessOutput{{Output: &ateenvv1alpha.ProcessOutput_Exit{Exit: tt.exit}}}}
			result := receiveOutput(stream, "p", false, &bytes.Buffer{}, &bytes.Buffer{})
			if result.err == nil || !strings.Contains(result.err.Error(), tt.want) {
				t.Fatalf("result = %+v, want error containing %q", result, tt.want)
			}
		})
	}
}

func TestReceiveOutputMapsFragmentedFramesAndRejectsPartial(t *testing.T) {
	allowed := `{"jsonrpc":"2.0","method":"session/update","params":{}}` + "\n"
	stream := &outputStreamFixture{messages: []*ateenvv1alpha.ProcessOutput{
		{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(allowed[:17])}},
		{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(allowed[17:])}},
		{Output: &ateenvv1alpha.ProcessOutput_Stderr{Stderr: []byte("diagnostic\n")}},
		exitedOutput("p", 0),
	}}
	var stdout, stderr bytes.Buffer
	result := receiveOutput(stream, "p", true, &stdout, &stderr)
	if result.err != nil || result.exitCode != 0 {
		t.Fatalf("result = %+v, want clean exit", result)
	}
	if stdout.String() != allowed || stderr.String() != "diagnostic\n" {
		t.Fatalf("output = %q/%q", stdout.String(), stderr.String())
	}

	partial := &outputStreamFixture{messages: []*ateenvv1alpha.ProcessOutput{
		{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(`{"jsonrpc":"2.0"}`)}},
		exitedOutput("p", 0),
	}}
	if result := receiveOutput(partial, "p", true, &bytes.Buffer{}, &bytes.Buffer{}); result.err == nil || !strings.Contains(result.err.Error(), "complete newline-delimited") {
		t.Fatalf("partial result = %+v, want incomplete-frame error", result)
	}

	unsupported := &outputStreamFixture{messages: []*ateenvv1alpha.ProcessOutput{
		{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte(`{"jsonrpc":"2.0","id":1,"method":"fs/read_text_file","params":{}}` + "\n")}},
		exitedOutput("p", 0),
	}}
	if result := receiveOutput(unsupported, "p", true, &bytes.Buffer{}, &bytes.Buffer{}); result.err == nil || !strings.Contains(result.err.Error(), "unsupported client method") {
		t.Fatalf("unsupported result = %+v, want capability error", result)
	}
}

type inputStreamFixture struct {
	requests []*ateenvv1alpha.WriteProcessInputRequest
}

func (s *inputStreamFixture) Send(request *ateenvv1alpha.WriteProcessInputRequest) error {
	s.requests = append(s.requests, request)
	return nil
}

func (s *inputStreamFixture) CloseAndRecv() (*ateenvv1alpha.WriteProcessInputResponse, error) {
	return &ateenvv1alpha.WriteProcessInputResponse{}, nil
}

func TestForwardInputChunksAtMost64KiB(t *testing.T) {
	data := strings.Repeat("x", maxForwardChunk*2+17)
	stream := new(inputStreamFixture)
	if err := forwardInput(strings.NewReader(data), stream, "p", "", make(chan time.Time, 1)); err != nil {
		t.Fatal(err)
	}
	if len(stream.requests) < 4 {
		t.Fatalf("requests = %d, want data chunks plus close", len(stream.requests))
	}
	for i, request := range stream.requests {
		if len(request.GetData()) > maxForwardChunk {
			t.Fatalf("request %d data = %d bytes", i, len(request.GetData()))
		}
		if i == 0 && request.GetProcessId() != "p" {
			t.Fatalf("first process id = %q", request.GetProcessId())
		}
		if i > 0 && request.GetProcessId() != "" {
			t.Fatalf("request %d unexpectedly repeats process id", i)
		}
	}
	if !stream.requests[len(stream.requests)-1].GetClose() {
		t.Fatal("last request did not close stdin")
	}
}

func TestProxyProcessPreservesMalformedInputFailure(t *testing.T) {
	_, _, address := simulatedServer(t, simulatedStop, nil)
	cfg := simulatedConfig(address)
	cfg.ACPCWD = "/workspace/work"
	cfg.ShutdownTimeout = 20 * time.Millisecond
	cfg.ProcessTimeout = time.Second
	_, err := runWithSignals(context.Background(), cfg, strings.NewReader("{"), &bytes.Buffer{}, &bytes.Buffer{}, nil)
	if err == nil || !strings.Contains(err.Error(), "unterminated NDJSON") {
		t.Fatalf("error = %v, want malformed-input failure", err)
	}
}

func TestProxyProcessEOFDeadlineEscalatesToKill(t *testing.T) {
	_, process, address := simulatedServer(t, simulatedStop, nil)
	cfg := simulatedConfig(address)
	cfg.ShutdownTimeout = 20 * time.Millisecond
	cfg.ProcessTimeout = time.Second
	code, err := runWithSignals(context.Background(), cfg, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{}, nil)
	if err != nil || code != 130 {
		t.Fatalf("result = %d/%v, want SIGKILL-triggered exit", code, err)
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.signalCalls != 1 || process.lastSignal != ateenvv1alpha.Signal_SIGNAL_KILL {
		t.Fatalf("signals = %d/%v, want one SIGKILL", process.signalCalls, process.lastSignal)
	}
}

func TestProxyProcessSecondSignalEscalatesToKill(t *testing.T) {
	_, process, address := simulatedServer(t, simulatedEscalate, nil)
	cfg := simulatedConfig(address)
	release := make(chan struct{})
	signals := make(chan os.Signal, 2)
	result := make(chan struct {
		code int
		err  error
	}, 1)
	go func() {
		code, err := runWithSignals(context.Background(), cfg, releaseReader{release: release}, &bytes.Buffer{}, &bytes.Buffer{}, signals)
		result <- struct {
			code int
			err  error
		}{code, err}
	}()
	waitForSimulatedStart(t, process)
	signals <- os.Interrupt
	waitForSimulatedSignal(t, process, 1)
	signals <- os.Interrupt
	select {
	case got := <-result:
		if got.err != nil || got.code != 130 {
			t.Fatalf("result = %d/%v, want escalated exit", got.code, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for escalated process")
	}
	close(release)
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.signalCalls != 2 || process.lastSignal != ateenvv1alpha.Signal_SIGNAL_KILL {
		t.Fatalf("signals = %d/%v, want INT then KILL", process.signalCalls, process.lastSignal)
	}
}

func waitForSimulatedStart(t *testing.T, process *simulatedProcess) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		process.mu.Lock()
		started := process.startCalls > 0
		process.mu.Unlock()
		if started {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for simulated start")
		case <-time.After(time.Millisecond):
		}
	}
}

func waitForSimulatedSignal(t *testing.T, process *simulatedProcess, count int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		process.mu.Lock()
		calls := process.signalCalls
		process.mu.Unlock()
		if calls >= count {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for signal %d", count)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestProxyProcessUsesFiniteGuestTimeout(t *testing.T) {
	_, process, address := simulatedServer(t, simulatedDuplex, nil)
	cfg := simulatedConfig(address)
	cfg.ProcessTimeout = 17 * time.Second
	if _, err := runWithSignals(context.Background(), cfg, strings.NewReader("input"), &bytes.Buffer{}, &bytes.Buffer{}, nil); err != nil {
		t.Fatal(err)
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if got := process.startReq.GetTimeout().AsDuration(); got != cfg.ProcessTimeout {
		t.Fatalf("guest timeout = %s, want %s", got, cfg.ProcessTimeout)
	}
}

func TestWriteAllRejectsShortWriter(t *testing.T) {
	err := writeAll(shortWriter{}, []byte("x"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("error = %v, want short write", err)
	}
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) { return 0, nil }

// A broken parent's stdout/stderr must not prevent cancellation or cause cleanup
// diagnostics to block behind that same writer. Release happens only after return.
func TestCancellationWithBlockedOutputAndFailedCleanup(t *testing.T) {
	for _, blockStderr := range []bool{false, true} {
		t.Run(fmt.Sprintf("stderr=%v", blockStderr), func(t *testing.T) {
			_, process, address := simulatedServer(t, simulatedDuplex, nil)
			process.signalErr = errors.New("simulated cleanup unavailable")
			writer := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
			defer close(writer.release)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stdout, stderr := io.Writer(writer), io.Writer(io.Discard)
			if blockStderr {
				stdout, stderr = stderr, stdout
			}
			done := make(chan error, 1)
			go func() {
				_, err := runWithSignals(ctx, simulatedConfig(address), strings.NewReader("input"), stdout, stderr, nil)
				done <- err
			}()
			select {
			case <-writer.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("writer not reached")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "cleanup unavailable") {
					t.Fatalf("lost cancellation/cleanup error: %v", err)
				}
			case <-time.After(4 * time.Second):
				t.Fatal("blocked output prevented cancellation")
			}
		})
	}
}

func TestLocalProcessWatchdogBoundsBlockedOutputAndInput(t *testing.T) {
	_, process, address := simulatedServer(t, simulatedMissingExit, nil)
	cfg := simulatedConfig(address)
	cfg.ProcessTimeout = 250 * time.Millisecond
	cfg.ShutdownTimeout = 50 * time.Millisecond
	stdin, stdinWriter := io.Pipe()
	writer := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	defer func() {
		close(writer.release)
		_ = stdinWriter.Close()
	}()

	done := make(chan error, 1)
	started := time.Now()
	go func() {
		_, err := runWithSignals(context.Background(), cfg, stdin, writer, io.Discard, nil)
		done <- err
	}()
	select {
	case <-writer.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("writer not reached")
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "local process timeout") {
			t.Fatalf("error = %v, want local process timeout", err)
		}
		if elapsed := time.Since(started); elapsed > 4*time.Second {
			t.Fatalf("watchdog returned after %s", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("local watchdog did not bound blocked I/O")
	}

	process.mu.Lock()
	defer process.mu.Unlock()
	if process.signalCalls != 1 || process.lastSignal != ateenvv1alpha.Signal_SIGNAL_KILL {
		t.Fatalf("signals = %d/%v, want one SIGKILL", process.signalCalls, process.lastSignal)
	}
}

type blockingWriter struct{ entered, release chan struct{} }

func (w *blockingWriter) Write(data []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(data), nil
}

func TestInputChunksOwnTheirBytes(t *testing.T) {
	input := strings.Repeat("a", maxForwardChunk) + strings.Repeat("b", maxForwardChunk) + "last"
	stream := new(inputStreamFixture)
	if err := forwardInput(strings.NewReader(input), stream, "p", "", make(chan time.Time, 1)); err != nil {
		t.Fatal(err)
	}
	var got []byte
	for _, request := range stream.requests {
		got = append(got, request.Data...)
	}
	if string(got) != input {
		t.Fatal("previously sent buffers were mutated by the next read")
	}
}
