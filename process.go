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
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	defaultShutdownTimeout = 10 * time.Second
	defaultProcessTimeout  = time.Hour
	startTimeout           = 10 * time.Second
	signalTimeout          = 5 * time.Second
	cleanupTimeout         = time.Second
	maxForwardChunk        = 64 << 10
)

type outputResult struct {
	exitCode int
	err      error
}

// There is one output goroutine. Closing this gate suppresses subsequent writes
// without waiting for a currently blocked user writer. An in-flight Write cannot
// be interrupted through io.Writer; the CLI exits after bounded cleanup.
type outputGate struct {
	writer io.Writer
	closed atomic.Bool
}

func newOutputGate(writer io.Writer) *outputGate { return &outputGate{writer: writer} }

func (g *outputGate) Write(data []byte) (int, error) {
	if g.closed.Load() || g.writer == nil {
		return len(data), nil
	}
	return g.writer.Write(data)
}

func (g *outputGate) close() {
	g.closed.Store(true)
}

// proxyProcess owns exactly one remote process. It deliberately never retries
// StartProcess: an RPC error may mean that the process was created remotely.
func proxyProcess(ctx context.Context, process ateenvv1alpha.ProcessServiceClient, cfg Config, stdin io.Reader, stdout, stderr io.Writer, signals <-chan os.Signal) (int, error) {
	processStartedAt := time.Now()

	processTimeout := cfg.ProcessTimeout
	if processTimeout <= 0 {
		processTimeout = defaultProcessTimeout
	}
	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	processDeadline := processStartedAt.Add(processTimeout)
	// The guest timeout is authoritative when it reports an exit. The local
	// deadline is a conservative fallback; keep the process context alive for
	// one bounded shutdown grace so a native timeout exit can still arrive.
	processCtx, cancelProcess := context.WithDeadline(ctx, processDeadline.Add(shutdownTimeout))
	defer cancelProcess()

	// There is deliberately one StartProcess call. An ambiguous RPC result is
	// surfaced to the operator; retrying could create a second remote process.
	startCtx, cancelStart := context.WithTimeout(processCtx, startTimeout)
	proc, err := process.StartProcess(startCtx, &ateenvv1alpha.StartProcessRequest{
		Command: commandCopy(cfg.Command),
		Stdin:   true,
		Timeout: durationpb.New(processTimeout),
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
	if cfg.OnProcessStarted != nil {
		if err := cfg.OnProcessStarted(processID); err != nil {
			return 1, withCleanupError(fmt.Errorf("recording started remote process: %w", err), terminate(process, processID))
		}
	}

	stdoutGate := newOutputGate(stdout)
	stderrGate := newOutputGate(stderr)
	output, err := process.StreamProcessOutput(processCtx, &ateenvv1alpha.StreamProcessOutputRequest{
		ProcessId: processID,
		Follow:    true,
	})
	if err != nil {
		cleanupErr := terminate(process, processID)
		stdoutGate.close()
		stderrGate.close()
		return 1, withCleanupError(fmt.Errorf("streaming remote process output: %w", err), cleanupErr)
	}

	input, err := process.WriteProcessInput(processCtx)
	if err != nil {
		cleanupErr := terminate(process, processID)
		stdoutGate.close()
		stderrGate.close()
		return 1, withCleanupError(fmt.Errorf("opening remote process stdin: %w", err), cleanupErr)
	}

	inputErr := make(chan error, 1)
	inputDone := make(chan struct{})
	eofStarted := make(chan time.Time, 1)
	go func() {
		defer close(inputDone)
		inputErr <- forwardInput(stdin, input, processID, cfg.ACPCWD, eofStarted)
	}()
	outputDone := make(chan outputResult, 1)
	go func() {
		outputDone <- receiveOutput(output, processID, cfg.ACPCWD != "", stdoutGate, stderrGate)
	}()

	var inputCh <-chan error = inputErr
	var eofCh <-chan time.Time = eofStarted
	var outputCh <-chan outputResult = outputDone
	var shutdownTimer *time.Timer
	var shutdownC <-chan time.Time
	var shutdownDeadline time.Time
	var inputFailure error
	var firstSignal bool
	var killSent bool

	setDeadline := func(deadline time.Time) {
		if !shutdownDeadline.IsZero() && !deadline.Before(shutdownDeadline) {
			return
		}
		shutdownDeadline = deadline
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		if shutdownTimer == nil {
			shutdownTimer = time.NewTimer(remaining)
		} else {
			if !shutdownTimer.Stop() {
				select {
				case <-shutdownTimer.C:
				default:
				}
			}
			shutdownTimer.Reset(remaining)
		}
		shutdownC = shutdownTimer.C
	}
	resetDeadline := func(deadline time.Time) {
		shutdownDeadline = time.Time{}
		setDeadline(deadline)
	}
	stopTimer := func() {
		if shutdownTimer != nil {
			shutdownTimer.Stop()
		}
		shutdownC = nil
	}

	// cancelAndWait bounds cleanup of gRPC goroutines. An arbitrary io.Reader
	// may still be blocked in Read; the CLI owns os.Stdin and closes it only at
	// process exit, while library callers retain ownership of their reader.
	finish := func(code int, finishErr error, outputConsumed bool) (int, error) {
		stopTimer()
		cancelProcess()
		if !outputConsumed {
			waitChannel(outputDone, cleanupTimeout)
		}
		waitChannel(inputDone, cleanupTimeout)
		stdoutGate.close()
		stderrGate.close()
		return code, finishErr
	}

	for {
		select {
		case <-ctx.Done():
			return finish(1, withCleanupError(ctx.Err(), terminate(process, processID)), false)

		case <-processCtx.Done():
			if ctx.Err() != nil {
				return finish(1, withCleanupError(ctx.Err(), terminate(process, processID)), false)
			}
			timeoutErr := fmt.Errorf("remote process exceeded local process timeout of %s", processTimeout)
			return finish(1, withCleanupError(timeoutErr, terminate(process, processID)), false)

		case result := <-outputCh:
			outputCh = nil
			if inputFailure == nil {
				// Prefer an already-completed stdin validation error over a
				// nearly simultaneous remote exit. This prevents malformed ACP
				// input from being reported as a successful run.
				select {
				case inputErrValue := <-inputCh:
					inputCh = nil
					inputFailure = inputErrValue
				default:
				}
			}
			if result.err != nil {
				cleanupErr := terminate(process, processID)
				if inputFailure != nil {
					return finish(1, withCleanupError(inputFailure, cleanupErr), true)
				}
				if ctx.Err() != nil {
					return finish(1, withCleanupError(ctx.Err(), cleanupErr), true)
				}
				return finish(1, withCleanupError(result.err, cleanupErr), true)
			}
			if inputFailure != nil {
				return finish(result.exitCode, inputFailure, true)
			}
			return finish(result.exitCode, nil, true)

		case startedAt := <-eofCh:
			eofCh = nil
			// The timestamp is captured before CloseAndRecv, so a stuck close
			// acknowledgement cannot consume an unbounded amount of grace time.
			setDeadline(startedAt.Add(shutdownTimeout))

		case err := <-inputCh:
			inputCh = nil
			if err == nil {
				// EOF was delivered to the process. Its output stream remains
				// authoritative for completion and exit status.
				continue
			}
			inputFailure = err
			if ctx.Err() != nil {
				return finish(1, withCleanupError(ctx.Err(), terminate(process, processID)), false)
			}
			setDeadline(time.Now().Add(shutdownTimeout))
			if !firstSignal {
				firstSignal = true
				if signalErr := sendGuestSignal(process, processID, ateenvv1alpha.Signal_SIGNAL_TERM, signalTimeout); signalErr != nil {
					inputFailure = fmt.Errorf("%w; cleanup signal: %v", inputFailure, signalErr)
				}
			}

		case sig, ok := <-signals:
			if !ok {
				signals = nil
				continue
			}
			if killSent {
				continue
			}
			if firstSignal {
				killSent = true
				if signalErr := sendGuestSignal(process, processID, ateenvv1alpha.Signal_SIGNAL_KILL, signalTimeout); signalErr != nil {
					if inputFailure == nil {
						inputFailure = fmt.Errorf("escalating remote process signal: %w", signalErr)
					} else {
						inputFailure = fmt.Errorf("%w; SIGKILL: %v", inputFailure, signalErr)
					}
				}
				resetDeadline(time.Now().Add(shutdownTimeout))
				continue
			}
			firstSignal = true
			setDeadline(time.Now().Add(shutdownTimeout))
			if signalErr := signalProcess(process, processID, sig); signalErr != nil {
				inputFailure = fmt.Errorf("signalling remote process: %w", signalErr)
			}

		case <-shutdownC:
			if killSent {
				if inputFailure != nil {
					return finish(1, fmt.Errorf("%w; remote process did not report exit after SIGKILL", inputFailure), false)
				}
				return finish(1, errors.New("remote process did not report exit after SIGKILL"), false)
			}
			killSent = true
			if signalErr := sendGuestSignal(process, processID, ateenvv1alpha.Signal_SIGNAL_KILL, signalTimeout); signalErr != nil {
				if inputFailure == nil {
					inputFailure = fmt.Errorf("escalating remote process signal: %w", signalErr)
				} else {
					inputFailure = fmt.Errorf("%w; SIGKILL: %v", inputFailure, signalErr)
				}
			}
			resetDeadline(time.Now().Add(shutdownTimeout))
		}
	}
}

func commandCopy(command []string) []string { return append([]string(nil), command...) }

func waitChannel[T any](done <-chan T, timeout time.Duration) {
	if done == nil {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}

type inputSender struct {
	stream    grpcClientInputStream
	processID string
	sentID    bool
}

// grpcClientInputStream is the generated client-streaming subset used here;
// naming it keeps the lifecycle helpers easy to fake in focused tests.
type grpcClientInputStream interface {
	Send(*ateenvv1alpha.WriteProcessInputRequest) error
	CloseAndRecv() (*ateenvv1alpha.WriteProcessInputResponse, error)
}

func (s *inputSender) send(data []byte, closeInput bool) error {
	req := &ateenvv1alpha.WriteProcessInputRequest{Data: append([]byte(nil), data...), Close: closeInput}
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

func forwardInput(stdin io.Reader, stream grpcClientInputStream, processID, acpCWD string, eofStarted chan<- time.Time) error {
	sender := &inputSender{stream: stream, processID: processID}
	markEOF := func() {
		select {
		case eofStarted <- time.Now():
		default:
		}
	}
	if acpCWD != "" {
		if err := transformACPInput(stdin, acpCWD, func(data []byte) error {
			return sendChunks(sender, data)
		}); err != nil {
			return err
		}
		markEOF()
		return sender.close()
	}
	return forwardRawInput(stdin, sender, markEOF)
}

func sendChunks(sender *inputSender, data []byte) error {
	for len(data) > 0 {
		n := len(data)
		if n > maxForwardChunk {
			n = maxForwardChunk
		}
		if err := sender.send(data[:n], false); err != nil {
			return fmt.Errorf("writing remote process stdin: %w", err)
		}
		data = data[n:]
	}
	return nil
}

func forwardRawInput(stdin io.Reader, sender *inputSender, markEOF func()) error {
	buf := make([]byte, maxForwardChunk)
	for {
		n, readErr := stdin.Read(buf)
		if n > 0 {
			if err := sendChunks(sender, buf[:n]); err != nil {
				return err
			}
		}
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			markEOF()
			if err := sender.close(); err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("reading ACP stdin: %w", readErr)
	}
}

type outputFrameValidator struct {
	line []byte
}

func (v *outputFrameValidator) feed(data []byte, emit func([]byte) error) error {
	for len(data) > 0 {
		newline := bytes.IndexByte(data, '\n')
		if newline < 0 {
			v.line = append(v.line, data...)
			if len(v.line) > maxACPLineBytes {
				return fmt.Errorf("ACP output line exceeds %d bytes", maxACPLineBytes)
			}
			return nil
		}
		v.line = append(v.line, data[:newline+1]...)
		if len(v.line) > maxACPLineBytes {
			return fmt.Errorf("ACP output line exceeds %d bytes", maxACPLineBytes)
		}
		if err := v.emitLine(emit); err != nil {
			return err
		}
		v.line = nil
		data = data[newline+1:]
	}
	return nil
}

func (v *outputFrameValidator) emitLine(emit func([]byte) error) error {
	line := v.line
	payload := line[:len(line)-1]
	if len(payload) > 0 && payload[len(payload)-1] == '\r' {
		payload = payload[:len(payload)-1]
	}
	if len(bytes.TrimSpace(payload)) == 0 {
		return errors.New("ACP output contains a blank line")
	}
	if err := validateACPOutputLine(payload); err != nil {
		return err
	}
	return emit(append([]byte(nil), line...))
}

func (v *outputFrameValidator) flush() error {
	if len(v.line) != 0 {
		return errors.New("ACP output ended before a complete newline-delimited frame")
	}
	return nil
}

func receiveOutput(stream interface {
	Recv() (*ateenvv1alpha.ProcessOutput, error)
}, processID string, mapped bool, stdout, stderr io.Writer) outputResult {
	var frames outputFrameValidator
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return outputResult{err: errors.New("remote process output ended before an exit message")}
		}
		if err != nil {
			return outputResult{err: fmt.Errorf("receiving remote process output: %w", err)}
		}
		if message == nil {
			return outputResult{err: errors.New("remote process output contained an empty message")}
		}
		if data := message.GetStdout(); len(data) > 0 {
			write := func(frame []byte) error { return writeAll(stdout, frame) }
			if mapped {
				if err := frames.feed(data, write); err != nil {
					return outputResult{err: fmt.Errorf("validating ACP stdout: %w", err)}
				}
			} else if err := write(data); err != nil {
				return outputResult{err: fmt.Errorf("writing ACP stdout: %w", err)}
			}
		}
		if data := message.GetStderr(); len(data) > 0 {
			if err := writeAll(stderr, data); err != nil {
				return outputResult{err: fmt.Errorf("writing remote stderr: %w", err)}
			}
		}
		if exit := message.GetExit(); exit != nil {
			if mapped {
				if err := frames.flush(); err != nil {
					return outputResult{err: err}
				}
			}
			if exit.GetProcessId() != processID {
				return outputResult{err: fmt.Errorf("remote process exit id %q does not match %q", exit.GetProcessId(), processID)}
			}
			if exit.GetState() != ateenvv1alpha.ProcessState_PROCESS_STATE_EXITED {
				return outputResult{err: fmt.Errorf("remote process %s exit message has state %s", processID, exit.GetState())}
			}
			if exit.GetExitCode() < 0 || exit.GetExitCode() > 255 {
				return outputResult{err: fmt.Errorf("remote process %s returned invalid exit code %d", processID, exit.GetExitCode())}
			}
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
	return sendGuestSignal(process, processID, guestSig, signalTimeout)
}

func sendGuestSignal(process ateenvv1alpha.ProcessServiceClient, processID string, sig ateenvv1alpha.Signal, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := process.SignalProcess(ctx, &ateenvv1alpha.SignalProcessRequest{
		ProcessId: processID,
		Signal:    sig,
	})
	return err
}

func withCleanupError(primary, cleanup error) error {
	if cleanup == nil {
		return primary
	}
	if primary == nil {
		return fmt.Errorf("cleanup signal: %w", cleanup)
	}
	return fmt.Errorf("%w; cleanup signal: %v", primary, cleanup)
}

// terminate is used after an unrecoverable transport failure. SIGKILL keeps a
// failed output/input stream from leaving a remote process for its full native
// timeout; graceful EOF and user signals follow the TERM-then-KILL path above.
func terminate(process ateenvv1alpha.ProcessServiceClient, processID string) error {
	return sendGuestSignal(process, processID, ateenvv1alpha.Signal_SIGNAL_KILL, 2*time.Second)
}

// run installs the real OS signal handler for callers that do not need to
// provide a signal channel (tests use runWithSignals directly).
func run(ctx context.Context, cfg Config, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	signals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	return runWithSignals(ctx, cfg, stdin, stdout, stderr, signals)
}
