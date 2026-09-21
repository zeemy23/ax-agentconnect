// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// These tests use real grpc servers and the pinned generated AX/env clients,
// but the services are in-process fakes. They are simulated transport evidence,
// not acceptance of a live AX cluster or a real agent.
type simulatedAX struct {
	v1alpha1.UnimplementedAXServer
	task  *v1alpha1.Task
	calls int
}

func (s *simulatedAX) GetTask(_ context.Context, req *v1alpha1.GetTaskRequest) (*v1alpha1.Task, error) {
	s.calls++
	if req.GetAtespace() != "default" || req.GetName() != "fixed-task" {
		return nil, status.Error(codes.NotFound, "unexpected task lookup")
	}
	return s.task, nil
}

type simulatedProcessMode int

const (
	simulatedDuplex simulatedProcessMode = iota
	simulatedStop
	simulatedMissingExit
	simulatedStartError
)

type simulatedProcess struct {
	ateenvv1alpha.UnimplementedProcessServiceServer
	mode          simulatedProcessMode
	startErr      error
	expectedActor string

	mu          sync.Mutex
	startCalls  int
	startReq    *ateenvv1alpha.StartProcessRequest
	input       []byte
	signalCalls int
	lastSignal  ateenvv1alpha.Signal

	inputDone  chan struct{}
	signalDone chan struct{}
	inputOnce  sync.Once
	signalOnce sync.Once
}

func (s *simulatedProcess) actorOK(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || len(md.Get("ate-target-actor")) != 1 || md.Get("ate-target-actor")[0] != s.expectedActor {
		return status.Errorf(codes.PermissionDenied, "wrong actor metadata: %v", md.Get("ate-target-actor"))
	}
	return nil
}

func (s *simulatedProcess) StartProcess(ctx context.Context, req *ateenvv1alpha.StartProcessRequest) (*ateenvv1alpha.Process, error) {
	if err := s.actorOK(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.startCalls++
	s.startReq = req
	s.mu.Unlock()
	if s.startErr != nil {
		return nil, s.startErr
	}
	return &ateenvv1alpha.Process{ProcessId: "simulated-process", State: ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING}, nil
}

func (s *simulatedProcess) StreamProcessOutput(req *ateenvv1alpha.StreamProcessOutputRequest, stream grpc.ServerStreamingServer[ateenvv1alpha.ProcessOutput]) error {
	if err := s.actorOK(stream.Context()); err != nil {
		return err
	}
	if req.GetProcessId() != "simulated-process" || !req.GetFollow() {
		return status.Error(codes.InvalidArgument, "unexpected output request")
	}
	if s.mode == simulatedMissingExit {
		if err := stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: []byte("partial")}}); err != nil {
			return err
		}
		return nil
	}
	if s.mode == simulatedStop {
		<-s.signalDone
		return stream.Send(&ateenvv1alpha.ProcessOutput{
			Output: &ateenvv1alpha.ProcessOutput_Exit{Exit: &ateenvv1alpha.Process{ProcessId: "simulated-process", State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, ExitCode: 130}},
		})
	}
	<-s.inputDone
	s.mu.Lock()
	echo := append([]byte("echo:"), s.input...)
	s.mu.Unlock()
	if err := stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stdout{Stdout: echo}}); err != nil {
		return err
	}
	if err := stream.Send(&ateenvv1alpha.ProcessOutput{Output: &ateenvv1alpha.ProcessOutput_Stderr{Stderr: []byte("remote stderr\n")}}); err != nil {
		return err
	}
	return stream.Send(&ateenvv1alpha.ProcessOutput{
		Output: &ateenvv1alpha.ProcessOutput_Exit{Exit: &ateenvv1alpha.Process{ProcessId: "simulated-process", State: ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED, ExitCode: 7}},
	})
}

func (s *simulatedProcess) WriteProcessInput(stream grpc.ClientStreamingServer[ateenvv1alpha.WriteProcessInputRequest, ateenvv1alpha.WriteProcessInputResponse]) error {
	if err := s.actorOK(stream.Context()); err != nil {
		return err
	}
	first := true
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&ateenvv1alpha.WriteProcessInputResponse{})
		}
		if err != nil {
			return err
		}
		if first && req.GetProcessId() != "simulated-process" {
			return status.Error(codes.InvalidArgument, "missing process id on first input message")
		}
		first = false
		s.mu.Lock()
		s.input = append(s.input, req.GetData()...)
		s.mu.Unlock()
		if req.GetClose() {
			s.inputOnce.Do(func() { close(s.inputDone) })
			return stream.SendAndClose(&ateenvv1alpha.WriteProcessInputResponse{})
		}
	}
}

func (s *simulatedProcess) SignalProcess(ctx context.Context, req *ateenvv1alpha.SignalProcessRequest) (*ateenvv1alpha.Process, error) {
	if err := s.actorOK(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.signalCalls++
	s.lastSignal = req.GetSignal()
	s.mu.Unlock()
	s.signalOnce.Do(func() { close(s.signalDone) })
	return &ateenvv1alpha.Process{ProcessId: req.GetProcessId(), State: ateenvv1alpha.ProcessState_PROCESS_STATE_RUNNING}, nil
}

func simulatedServer(t *testing.T, mode simulatedProcessMode, startErr error) (*simulatedAX, *simulatedProcess, string) {
	t.Helper()
	ax := &simulatedAX{task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "fixed-task", Atespace: "default"},
		Spec:     &v1alpha1.TaskSpec{Debug: true},
		Status:   &v1alpha1.TaskStatus{Phase: "Running", Actor: "fixed-actor"},
	}}
	process := &simulatedProcess{
		mode:          mode,
		startErr:      startErr,
		expectedActor: "default/fixed-actor",
		inputDone:     make(chan struct{}),
		signalDone:    make(chan struct{}),
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	v1alpha1.RegisterAXServer(server, ax)
	ateenvv1alpha.RegisterProcessServiceServer(server, process)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(func() {
		server.Stop()
		_ = lis.Close()
	})
	return ax, process, lis.Addr().String()
}

func simulatedConfig(address string) Config {
	return Config{
		Task:     "fixed-task",
		Atespace: "default",
		Server:   address,
		Router:   address,
		Command:  []string{"agent", "acp"},
	}
}

func TestSimulatedDuplexBytesAndSeparateStderr(t *testing.T) {
	ax, process, address := simulatedServer(t, simulatedDuplex, nil)
	var stdout, stderr bytes.Buffer
	exit, err := runWithSignals(context.Background(), simulatedConfig(address), strings.NewReader("hello ACP\n"), &stdout, &stderr, nil)
	if err != nil {
		t.Fatalf("runWithSignals() error = %v", err)
	}
	if exit != 7 {
		t.Fatalf("exit = %d, want 7", exit)
	}
	if got, want := stdout.String(), "echo:hello ACP\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	if got, want := stderr.String(), "remote stderr\n"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
	if ax.calls != 1 {
		t.Fatalf("GetTask calls = %d, want 1", ax.calls)
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.startCalls != 1 {
		t.Fatalf("StartProcess calls = %d, want 1", process.startCalls)
	}
	if process.startReq == nil || !process.startReq.GetStdin() {
		t.Fatal("StartProcess did not request stdin")
	}
}

type releaseReader struct{ release <-chan struct{} }

func (r releaseReader) Read([]byte) (int, error) {
	<-r.release
	return 0, io.EOF
}

func TestSimulatedStopSignalsRemoteProcess(t *testing.T) {
	_, process, address := simulatedServer(t, simulatedStop, nil)
	signals := make(chan os.Signal, 1)
	release := make(chan struct{})
	var stdout, stderr bytes.Buffer
	result := make(chan struct {
		code int
		err  error
	}, 1)
	go func() {
		code, err := runWithSignals(context.Background(), simulatedConfig(address), releaseReader{release: release}, &stdout, &stderr, signals)
		result <- struct {
			code int
			err  error
		}{code, err}
	}()

	signals <- os.Interrupt
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("runWithSignals() error = %v", got.err)
		}
		if got.code != 130 {
			t.Fatalf("exit = %d, want 130", got.code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for remote stop")
	}
	close(release)
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.signalCalls != 1 || process.lastSignal != ateenvv1alpha.Signal_SIGNAL_INT {
		t.Fatalf("signals = %d/%v, want one SIGINT", process.signalCalls, process.lastSignal)
	}
}

func TestSimulatedMissingExitIsRejected(t *testing.T) {
	_, process, address := simulatedServer(t, simulatedMissingExit, nil)
	var stdout, stderr bytes.Buffer
	_, err := runWithSignals(context.Background(), simulatedConfig(address), strings.NewReader("input"), &stdout, &stderr, nil)
	if err == nil || !strings.Contains(err.Error(), "before an exit message") {
		t.Fatalf("error = %v, want missing-exit failure", err)
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.startCalls != 1 {
		t.Fatalf("StartProcess calls = %d, want 1", process.startCalls)
	}
	if process.signalCalls != 1 {
		t.Fatalf("cleanup SignalProcess calls = %d, want 1", process.signalCalls)
	}
}

func TestSimulatedAmbiguousStartIsNotRetried(t *testing.T) {
	_, process, address := simulatedServer(t, simulatedStartError, status.Error(codes.Unavailable, "start result ambiguous"))
	var stdout, stderr bytes.Buffer
	_, err := runWithSignals(context.Background(), simulatedConfig(address), strings.NewReader("input"), &stdout, &stderr, nil)
	if err == nil || !strings.Contains(err.Error(), "start result ambiguous") {
		t.Fatalf("error = %v, want original start error", err)
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.startCalls != 1 {
		t.Fatalf("StartProcess calls = %d, want exactly one", process.startCalls)
	}
	if process.signalCalls != 0 {
		t.Fatalf("SignalProcess calls = %d, want none after failed start", process.signalCalls)
	}
}

func TestLoopbackDefaultRejectsNonLoopback(t *testing.T) {
	if err := validateEndpoint("router", "router.ate-system.svc.cluster.local:80", false); err == nil {
		t.Fatal("non-loopback endpoint accepted without explicit insecure-in-cluster flag")
	}
	if err := validateEndpoint("router", "router.ate-system.svc.cluster.local:80", true); err != nil {
		t.Fatalf("explicit insecure-in-cluster endpoint rejected: %v", err)
	}
}

func TestEndpointHostSupportsLoopbackURL(t *testing.T) {
	host, err := endpointHost("http://127.0.0.1:8080/")
	if err != nil || host != "127.0.0.1" {
		t.Fatalf("endpointHost() = %q, %v", host, err)
	}
	if normalizeEndpoint("https://localhost:8001/") != "localhost:8001" {
		t.Fatalf("normalizeEndpoint() did not strip URL wrapper")
	}
}
