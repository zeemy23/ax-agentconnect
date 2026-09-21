// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
)

const maxACPLineBytes = 4 << 20

// transformACPInput validates bounded NDJSON and applies the one operator
// mapping needed by an agent daemon running inside the task. It intentionally
// leaves all other requests byte-for-byte unchanged.
func transformACPInput(r io.Reader, cwd string, emit func([]byte) error) error {
	reader := bufio.NewReaderSize(r, 64*1024)
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		line = append(line, part...)
		if len(line) > maxACPLineBytes {
			return fmt.Errorf("ACP input line exceeds %d bytes", maxACPLineBytes)
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if len(line) > 0 {
			newline := []byte(nil)
			payload := line
			if payload[len(payload)-1] == '\n' {
				newline = []byte{'\n'}
				payload = payload[:len(payload)-1]
				if len(payload) > 0 && payload[len(payload)-1] == '\r' {
					newline = []byte{'\r', '\n'}
					payload = payload[:len(payload)-1]
				}
			}
			if len(bytes.TrimSpace(payload)) == 0 {
				return errors.New("ACP input contains a blank line")
			}
			mapped, mapErr := mapACPLine(payload, cwd)
			if mapErr != nil {
				return mapErr
			}
			if err := emit(append(mapped, newline...)); err != nil {
				return fmt.Errorf("forwarding ACP input: %w", err)
			}
			line = nil
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("reading ACP input: %w", err)
		}
	}
}

func mapACPLine(line []byte, cwd string) ([]byte, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(line, &envelope); err != nil || envelope == nil {
		if err == nil {
			err = errors.New("JSON object required")
		}
		return nil, fmt.Errorf("invalid ACP JSON-RPC request: %w", err)
	}
	var version string
	if raw, ok := envelope["jsonrpc"]; !ok || json.Unmarshal(raw, &version) != nil || version != "2.0" {
		return nil, errors.New("invalid ACP JSON-RPC request: jsonrpc must be \"2.0\"")
	}
	methodRaw, hasMethod := envelope["method"]
	if !hasMethod {
		return validateACPResponse(envelope, line)
	}
	var method string
	if json.Unmarshal(methodRaw, &method) != nil || method == "" {
		return nil, errors.New("invalid ACP JSON-RPC request: method must be a non-empty string")
	}
	if _, hasResult := envelope["result"]; hasResult {
		return nil, errors.New("invalid ACP JSON-RPC request: result is not allowed with method")
	}
	if _, hasError := envelope["error"]; hasError {
		return nil, errors.New("invalid ACP JSON-RPC request: error is not allowed with method")
	}
	if raw, ok := envelope["id"]; ok {
		if err := validateACPID(raw, "request"); err != nil {
			return nil, err
		}
	}

	targetMethod := method == "session/new" || method == "session/load"
	paramsRaw, hasParams := envelope["params"]
	var params map[string]json.RawMessage
	if hasParams {
		trimmed := bytes.TrimSpace(paramsRaw)
		if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
			return nil, errors.New("invalid ACP JSON-RPC request: params must be an object or array")
		}
		switch trimmed[0] {
		case '{':
			if err := json.Unmarshal(paramsRaw, &params); err != nil {
				return nil, errors.New("invalid ACP JSON-RPC request: params must be an object or array")
			}
		case '[':
			if targetMethod {
				return nil, errors.New("invalid ACP JSON-RPC request: target params must be an object")
			}
			return append([]byte(nil), line...), nil
		default:
			return nil, errors.New("invalid ACP JSON-RPC request: params must be an object or array")
		}
	}
	if params == nil {
		if !targetMethod {
			return append([]byte(nil), line...), nil
		}
		params = make(map[string]json.RawMessage)
	}
	if raw, ok := params["mcpServers"]; ok {
		var servers []json.RawMessage
		if err := json.Unmarshal(raw, &servers); err != nil {
			return nil, errors.New("ACP mcpServers must be an array")
		}
		if len(servers) != 0 {
			return nil, errors.New("ACP mcpServers are not supported by this bridge")
		}
	}
	if !targetMethod {
		return append([]byte(nil), line...), nil
	}
	encodedCWD, err := json.Marshal(cwd)
	if err != nil {
		return nil, fmt.Errorf("encoding ACP cwd: %w", err)
	}
	params["cwd"] = encodedCWD
	encodedParams, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("encoding ACP params: %w", err)
	}
	envelope["params"] = encodedParams
	return json.Marshal(envelope)
}

func validateACPResponse(envelope map[string]json.RawMessage, line []byte) ([]byte, error) {
	id, hasID := envelope["id"]
	if !hasID {
		return nil, errors.New("invalid ACP JSON-RPC response: id is required")
	}
	if err := validateACPID(id, "response"); err != nil {
		return nil, err
	}
	_, hasResult := envelope["result"]
	_, hasError := envelope["error"]
	if hasResult == hasError {
		return nil, errors.New("invalid ACP JSON-RPC response: exactly one of result or error is required")
	}
	return append([]byte(nil), line...), nil
}

func validateACPID(raw json.RawMessage, kind string) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return fmt.Errorf("invalid ACP JSON-RPC %s: id must be a string, number, or null", kind)
	}
	switch trimmed[0] {
	case '"', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'n':
		return nil
	default:
		return fmt.Errorf("invalid ACP JSON-RPC %s: id must be a string, number, or null", kind)
	}
}

func validateACPCWD(cwd string) error {
	if cwd == "" {
		return nil
	}
	if !filepath.IsAbs(cwd) {
		return fmt.Errorf("--acp-cwd must be an absolute path, got %q", cwd)
	}
	return nil
}
