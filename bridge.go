// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Config contains only operator-selected routing and process details. Nothing
// in the ACP stream can change these values.
type Config struct {
	Task              string
	Atespace          string
	Server            string
	Router            string
	Command           []string
	ACPCWD            string
	InsecureInCluster bool
}

func validateEndpoint(label, endpoint string, insecureInCluster bool) error {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(endpoint)), "https://") {
		return fmt.Errorf("%s: TLS/authentication is not implemented; use a plaintext endpoint", label)
	}
	host, err := endpointHost(endpoint)
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	if insecureInCluster || host == "localhost" || host == "localhost." {
		return nil
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("%s %q is non-loopback; use --insecure-in-cluster to allow plaintext upstream gRPC", label, endpoint)
}

func endpointHost(endpoint string) (string, error) {
	endpoint = normalizeEndpoint(endpoint)
	if endpoint == "" {
		return "", errors.New("address is required")
	}
	host, _, err := net.SplitHostPort(endpoint)
	if err == nil {
		if host == "" {
			return "", errors.New("address host is empty")
		}
		return host, nil
	}
	if strings.Contains(endpoint, ":") {
		return "", fmt.Errorf("address %q must be host:port", endpoint)
	}
	return endpoint, nil
}

func normalizeEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")
	return strings.TrimSuffix(endpoint, "/")
}

func runWithSignals(ctx context.Context, cfg Config, stdin io.Reader, stdout, stderr io.Writer, signals <-chan os.Signal) (int, error) {
	if err := validateConfig(cfg); err != nil {
		return 1, err
	}
	cfg.Server = normalizeEndpoint(cfg.Server)
	cfg.Router = normalizeEndpoint(cfg.Router)

	serverConn, err := grpc.NewClient(cfg.Server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 1, fmt.Errorf("dialing AX server: %w", err)
	}
	defer serverConn.Close()

	lookupCtx, cancelLookup := context.WithTimeout(ctx, 10*time.Second)
	task, err := v1alpha1.NewAXClient(serverConn).GetTask(lookupCtx, &v1alpha1.GetTaskRequest{
		Atespace: cfg.Atespace,
		Name:     cfg.Task,
	})
	cancelLookup()
	if err != nil {
		return 1, fmt.Errorf("getting task %s/%s: %w", cfg.Atespace, cfg.Task, err)
	}
	actor, err := checkTask(task, cfg)
	if err != nil {
		return 1, err
	}

	routerConn, err := dialRouter(cfg.Router, cfg.Atespace+"/"+actor)
	if err != nil {
		return 1, fmt.Errorf("dialing router: %w", err)
	}
	defer routerConn.Close()
	process := ateenvv1alpha.NewProcessServiceClient(routerConn)
	return proxyProcess(ctx, process, cfg.Command, cfg.ACPCWD, stdin, stdout, stderr, signals)
}

func validateConfig(cfg Config) error {
	if cfg.Task == "" {
		return errors.New("task is required")
	}
	if cfg.Atespace == "" {
		return errors.New("atespace is required")
	}
	if len(cfg.Command) == 0 || cfg.Command[0] == "" {
		return errors.New("command is required")
	}
	if err := validateACPCWD(cfg.ACPCWD); err != nil {
		return err
	}
	if err := validateEndpoint("AX server", cfg.Server, cfg.InsecureInCluster); err != nil {
		return err
	}
	return validateEndpoint("router", cfg.Router, cfg.InsecureInCluster)
}

func checkTask(task *v1alpha1.Task, cfg Config) (string, error) {
	if task == nil {
		return "", errors.New("AX returned an empty task")
	}
	if !task.GetSpec().GetDebug() {
		return "", fmt.Errorf("task %s/%s does not have spec.debug=true; guest process service is unavailable", cfg.Atespace, cfg.Task)
	}
	if task.GetStatus().GetPhase() != "Running" {
		return "", fmt.Errorf("task %s/%s is not running (phase %q)", cfg.Atespace, cfg.Task, task.GetStatus().GetPhase())
	}
	actor := task.GetStatus().GetActor()
	if actor == "" {
		return "", fmt.Errorf("task %s/%s has no running actor", cfg.Atespace, cfg.Task)
	}
	if strings.Contains(actor, "/") {
		return "", fmt.Errorf("task %s/%s returned an invalid actor %q", cfg.Atespace, cfg.Task, actor)
	}
	return actor, nil
}

func dialRouter(target, actor string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, "ate-target-actor", actor)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx = metadata.AppendToOutgoingContext(ctx, "ate-target-actor", actor)
			return streamer(ctx, desc, cc, method, opts...)
		}),
	)
}

type outputResult struct {
	exitCode int
	err      error
}

func proxyProcess(ctx context.Context, process ateenvv1alpha.ProcessServiceClient, command []string, acpCWD string, stdin io.Reader, stdout, stderr io.Writer, signals <-chan os.Signal) (int, error) {
	processCtx, cancelProcess := context.WithCancel(ctx)
	defer cancelProcess()

	// There is deliberately one StartProcess call. An ambiguous RPC result is
	// surfaced to the operator; retrying could create a second remote process.
	startCtx, cancelStart := context.WithTimeout(processCtx, 10*time.Second)
	proc, err := process.StartProcess(startCtx, &ateenvv1alpha.StartProcessRequest{
		Command: command,
		Stdin:   true,
	})
	cancelStart()
	if err != nil {
		return 1, fmt.Errorf("starting remote process: %w", err)
	}
	if proc == nil {
		return 1, errors.New("starting remote process returned no process")
	}
	processID := proc.GetProcessId()
	if processID == "" {
		return 1, errors.New("starting remote process returned no process id")
	}

	output, err := process.StreamProcessOutput(processCtx, &ateenvv1alpha.StreamProcessOutputRequest{
		ProcessId: processID,
		Follow:    true,
	})
	if err != nil {
		terminate(process, processID, stderr)
		return 1, fmt.Errorf("streaming remote process output: %w", err)
	}

	input, err := process.WriteProcessInput(processCtx)
	if err != nil {
		terminate(process, processID, stderr)
		return 1, fmt.Errorf("opening remote process stdin: %w", err)
	}

	inputErr := make(chan error, 1)
	go func() {
		inputErr <- forwardInput(stdin, input, processID, acpCWD)
	}()
	outputDone := make(chan outputResult, 1)
	go func() {
		outputDone <- receiveOutput(output, stdout, stderr)
	}()

	for {
		select {
		case result := <-outputDone:
			if result.err != nil {
				terminate(process, processID, stderr)
				return 1, result.err
			}
			return result.exitCode, nil
		case err := <-inputErr:
			if err == nil {
				// EOF was delivered to the process. Its output stream remains
				// authoritative for completion and exit status.
				inputErr = nil
				continue
			}
			terminate(process, processID, stderr)
			// A process can exit between the final stdin chunk and the guest's
			// close acknowledgement. Prefer its authoritative exit message when
			// it is already in flight; otherwise return the input failure.
			select {
			case result := <-outputDone:
				if result.err == nil {
					return result.exitCode, nil
				}
				return 1, result.err
			case <-time.After(2 * time.Second):
				return 1, err
			}
		case sig, ok := <-signals:
			if !ok {
				signals = nil
				continue
			}
			if err := signalProcess(process, processID, sig); err != nil {
				terminate(process, processID, stderr)
				return 1, fmt.Errorf("signalling remote process: %w", err)
			}
		}
	}
}

type inputSender struct {
	stream    grpc.ClientStreamingClient[ateenvv1alpha.WriteProcessInputRequest, ateenvv1alpha.WriteProcessInputResponse]
	processID string
	sentID    bool
}

func (s *inputSender) send(data []byte, closeInput bool) error {
	req := &ateenvv1alpha.WriteProcessInputRequest{Data: data, Close: closeInput}
	if !s.sentID {
		req.ProcessId = s.processID
		s.sentID = true
	}
	return s.stream.Send(req)
}

func (s *inputSender) close() error {
	if err := s.send(nil, true); err != nil {
		return fmt.Errorf("closing remote process stdin: %w", err)
	}
	if _, err := s.stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("acknowledging remote process stdin close: %w", err)
	}
	return nil
}

func forwardInput(stdin io.Reader, stream grpc.ClientStreamingClient[ateenvv1alpha.WriteProcessInputRequest, ateenvv1alpha.WriteProcessInputResponse], processID, acpCWD string) error {
	sender := &inputSender{stream: stream, processID: processID}
	if acpCWD != "" {
		if err := transformACPInput(stdin, acpCWD, func(data []byte) error {
			return sender.send(data, false)
		}); err != nil {
			return err
		}
		return sender.close()
	}
	return forwardRawInput(stdin, sender)
}

func forwardRawInput(stdin io.Reader, sender *inputSender) error {
	buf := make([]byte, 64*1024)
	for {
		n, readErr := stdin.Read(buf)
		if n > 0 {
			data := append([]byte(nil), buf[:n]...)
			if err := sender.send(data, false); err != nil {
				return fmt.Errorf("writing remote process stdin: %w", err)
			}
		}
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			if err := sender.close(); err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("reading ACP stdin: %w", readErr)
	}
}

func receiveOutput(stream grpc.ServerStreamingClient[ateenvv1alpha.ProcessOutput], stdout, stderr io.Writer) outputResult {
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return outputResult{err: errors.New("remote process output ended before an exit message")}
		}
		if err != nil {
			return outputResult{err: fmt.Errorf("receiving remote process output: %w", err)}
		}
		if data := message.GetStdout(); len(data) > 0 {
			if err := writeAll(stdout, data); err != nil {
				return outputResult{err: fmt.Errorf("writing ACP stdout: %w", err)}
			}
		}
		if data := message.GetStderr(); len(data) > 0 {
			if err := writeAll(stderr, data); err != nil {
				return outputResult{err: fmt.Errorf("writing remote stderr: %w", err)}
			}
		}
		if exit := message.GetExit(); exit != nil {
			return outputResult{exitCode: int(exit.GetExitCode())}
		}
	}
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func signalProcess(process ateenvv1alpha.ProcessServiceClient, processID string, sig os.Signal) error {
	guestSig := ateenvv1alpha.Signal_SIGNAL_TERM
	if sig == os.Interrupt {
		guestSig = ateenvv1alpha.Signal_SIGNAL_INT
	} else if sig == syscall.SIGQUIT {
		guestSig = ateenvv1alpha.Signal_SIGNAL_QUIT
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := process.SignalProcess(ctx, &ateenvv1alpha.SignalProcessRequest{
		ProcessId: processID,
		Signal:    guestSig,
	})
	return err
}

func terminate(process ateenvv1alpha.ProcessServiceClient, processID string, stderr io.Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := process.SignalProcess(ctx, &ateenvv1alpha.SignalProcessRequest{
		ProcessId: processID,
		Signal:    ateenvv1alpha.Signal_SIGNAL_TERM,
	})
	if err != nil && stderr != nil {
		fmt.Fprintf(stderr, "ax-agentconnect: cleanup signal for process %s: %v\n", processID, err)
	}
}

// run installs the real OS signal handler for callers that do not need to
// provide a signal channel (tests use runWithSignals directly).
func run(ctx context.Context, cfg Config, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	return runWithSignals(ctx, cfg, stdin, stdout, stderr, signals)
}
