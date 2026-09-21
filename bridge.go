// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	ateenvv1alpha "github.com/agent-substrate/env/proto/ateenv/v1alpha"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
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
	ServerTLS         transportConfig
	RouterTLS         transportConfig
	ShutdownTimeout   time.Duration
	ProcessTimeout    time.Duration
	StateDir          string
	ExpectedTaskID    string
	OnProcessStarted  func(string) error
}

func runWithSignals(ctx context.Context, cfg Config, stdin io.Reader, stdout, stderr io.Writer, signals <-chan os.Signal) (int, error) {
	if err := validateConfig(cfg); err != nil {
		return 1, err
	}
	if err := stopping(ctx, signals); err != nil {
		return 1, err
	}
	// Keep URL schemes for transport selection and durable receipt identity.

	serverConn, err := newConnection(cfg.Server, cfg.ServerTLS, cfg.InsecureInCluster)
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

	routerConn, err := dialRouter(cfg.Router, cfg.Atespace+"/"+actor, cfg.RouterTLS, cfg.InsecureInCluster)
	if err != nil {
		return 1, fmt.Errorf("dialing router: %w", err)
	}
	defer routerConn.Close()
	if err := stopping(ctx, signals); err != nil {
		return 1, err
	}
	process := ateenvv1alpha.NewProcessServiceClient(routerConn)
	// CLI always supplies StateDir. Empty is reserved for in-process transport tests.
	var guard *launchGuard
	if cfg.StateDir != "" {
		guard, err = acquireLaunch(cfg, actor, task.GetStatus().GetId())
		if err != nil {
			return 1, err
		}
		defer guard.close()
		cfg.OnProcessStarted = guard.started
	}
	code, runErr := proxyProcess(ctx, process, cfg, stdin, stdout, stderr, signals)
	if guard != nil && runErr == nil {
		if err := guard.complete(); err != nil {
			return 1, fmt.Errorf("remote exit confirmed but launch receipt cleanup failed: %w", err)
		}
	}
	if runErr != nil && guard != nil {
		return 1, fmt.Errorf("%w (launch receipt retained at %s)", runErr, guard.path)
	}
	return code, runErr
}

func validateConfig(cfg Config) error {
	if !validResourceName(cfg.Task) {
		return errors.New("task must be a nonempty resource name")
	}
	if !validResourceName(cfg.Atespace) {
		return errors.New("atespace must be a nonempty resource name")
	}
	if len(cfg.Command) == 0 || cfg.Command[0] == "" {
		return errors.New("command is required")
	}
	if cfg.ShutdownTimeout < 0 || cfg.ShutdownTimeout > 5*time.Minute || cfg.ProcessTimeout < 0 || cfg.ProcessTimeout > 24*time.Hour {
		return errors.New("timeouts must be positive, shutdown at most 5m and process at most 24h")
	}
	for _, arg := range cfg.Command {
		if strings.ContainsRune(arg, 0) {
			return errors.New("command contains NUL")
		}
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
	if task.GetMetadata().GetName() != cfg.Task || task.GetMetadata().GetAtespace() != cfg.Atespace {
		return "", errors.New("AX returned a different task identity")
	}
	if cfg.ExpectedTaskID != "" && task.GetStatus().GetId() != cfg.ExpectedTaskID {
		return "", errors.New("AX task status.id does not match --expected-task-id")
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
	if !validResourceName(actor) {
		return "", fmt.Errorf("task %s/%s returned an invalid actor %q", cfg.Atespace, cfg.Task, actor)
	}
	return actor, nil
}

func dialRouter(target, actor string, security transportConfig, allowPlaintext bool) (*grpc.ClientConn, error) {
	return newConnection(target, security, allowPlaintext,
		grpc.WithChainUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = actorContext(ctx, actor)
			return invoker(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithChainStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx = actorContext(ctx, actor)
			return streamer(ctx, desc, cc, method, opts...)
		}),
	)
}

func actorContext(ctx context.Context, actor string) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("ate-target-actor", actor)
	return metadata.NewOutgoingContext(ctx, md)
}
func validResourceName(value string) bool {
	if len(value) == 0 || len(value) > 253 || value == "." || value == ".." {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

func stopping(ctx context.Context, signals <-chan os.Signal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case sig, ok := <-signals:
		if ok {
			return fmt.Errorf("shutdown requested before launch (%s)", sig)
		}
	default:
	}
	return nil
}
