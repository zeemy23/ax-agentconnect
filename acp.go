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
	"path"
	"strings"
	"unicode/utf8"
)

const maxACPLineBytes = 4 << 20

// encoding/json accepts duplicate object members and replaces invalid UTF-8.
// Validate before mapping so a security decision is never made from a lossy
// parse. The depth cap keeps one bounded line from consuming unbounded stack.
const maxACPJSONDepth = 128

// transformACPInput validates bounded NDJSON and applies the operator cwd
// mapping plus initialize capability stripping needed by an agent daemon
// running inside the task. It intentionally leaves other requests unchanged.
func transformACPInput(r io.Reader, cwd string, emit func([]byte) error) error {
	if cwd == "" {
		return errors.New("mapped ACP cwd is required")
	}
	if err := validateACPCWD(cwd); err != nil {
		return err
	}
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
		// A non-EOF read error may leave only a prefix of a frame in line.
		// Return before mapping or emitting it.
		if err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("reading ACP input: %w", err)
		}
		if len(line) > 0 {
			if !bytes.HasSuffix(line, []byte{'\n'}) {
				return errors.New("ACP input ended with an unterminated NDJSON frame")
			}
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
			if len(mapped)+len(newline) > maxACPLineBytes {
				return fmt.Errorf("mapped ACP input line exceeds %d bytes", maxACPLineBytes)
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

func validateACPOutputLine(line []byte) error {
	if !utf8.Valid(line) {
		return errors.New("invalid ACP output JSON-RPC frame: input is not valid UTF-8")
	}
	if err := validateJSONDocument(line); err != nil {
		return fmt.Errorf("invalid ACP output JSON-RPC frame: %w", err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(line, &envelope); err != nil || envelope == nil {
		if err == nil {
			err = errors.New("JSON object required")
		}
		return fmt.Errorf("invalid ACP output JSON-RPC frame: %w", err)
	}
	var version string
	if raw, ok := envelope["jsonrpc"]; !ok || json.Unmarshal(raw, &version) != nil || version != "2.0" {
		return errors.New("invalid ACP output JSON-RPC frame: jsonrpc must be \"2.0\"")
	}
	rawMethod, hasMethod := envelope["method"]
	if !hasMethod {
		if _, err := validateACPResponse(envelope, line); err != nil {
			return fmt.Errorf("invalid ACP output JSON-RPC response: %w", err)
		}
		return nil
	}
	var method string
	if err := json.Unmarshal(rawMethod, &method); err != nil || method == "" {
		return errors.New("invalid ACP output JSON-RPC frame: method must be a non-empty string")
	}
	if _, hasResult := envelope["result"]; hasResult {
		return errors.New("invalid ACP output JSON-RPC frame: result is not allowed with method")
	}
	if _, hasError := envelope["error"]; hasError {
		return errors.New("invalid ACP output JSON-RPC frame: error is not allowed with method")
	}
	if rawID, ok := envelope["id"]; ok {
		if err := validateACPID(rawID, "output request"); err != nil {
			return err
		}
	}
	if rawParams, ok := envelope["params"]; ok {
		trimmed := bytes.TrimSpace(rawParams)
		if len(trimmed) == 0 || (trimmed[0] != '{' && trimmed[0] != '[') {
			return errors.New("invalid ACP output JSON-RPC request: params must be an object or array")
		}
	}
	if strings.HasPrefix(method, "fs/") || strings.HasPrefix(method, "terminal/") {
		return fmt.Errorf("ACP output requests unsupported client method %q", method)
	}
	return nil
}

func mapACPLine(line []byte, cwd string) ([]byte, error) {
	if !utf8.Valid(line) {
		return nil, errors.New("invalid ACP JSON-RPC request: input is not valid UTF-8")
	}
	if err := validateJSONDocument(line); err != nil {
		return nil, fmt.Errorf("invalid ACP JSON-RPC request: %w", err)
	}
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
	controlMethod := method == "initialize" || targetMethod
	paramsRaw, hasParams := envelope["params"]
	var params map[string]json.RawMessage
	if !hasParams {
		if controlMethod {
			return nil, errors.New("invalid ACP JSON-RPC request: control method requires object params")
		}
		return append([]byte(nil), line...), nil
	}
	trimmed := bytes.TrimSpace(paramsRaw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, errors.New("invalid ACP JSON-RPC request: params must be an object or array")
	}
	switch trimmed[0] {
	case '{':
		if err := json.Unmarshal(paramsRaw, &params); err != nil || params == nil {
			return nil, errors.New("invalid ACP JSON-RPC request: params must be an object or array")
		}
	case '[':
		if controlMethod {
			return nil, errors.New("invalid ACP JSON-RPC request: control method params must be an object")
		}
		return append([]byte(nil), line...), nil
	default:
		return nil, errors.New("invalid ACP JSON-RPC request: params must be an object or array")
	}
	if raw, ok := params["mcpServers"]; ok {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return nil, errors.New("invalid ACP mcpServers: null is not supported; use an empty array")
		}
		var servers []json.RawMessage
		if err := json.Unmarshal(raw, &servers); err != nil {
			return nil, errors.New("ACP mcpServers must be an array")
		}
		if len(servers) != 0 {
			return nil, errors.New("ACP mcpServers are not supported by this bridge")
		}
	}
	if method == "initialize" {
		return mapInitializeLine(envelope, params, line)
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
	mapped, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encoding ACP request: %w", err)
	}
	if len(mapped) > maxACPLineBytes {
		return nil, fmt.Errorf("mapped ACP input line exceeds %d bytes", maxACPLineBytes)
	}
	return mapped, nil
}

// mapInitializeLine removes only capabilities this byte bridge cannot service.
// Unknown and extension capability fields remain opaque and are carried through.
func mapInitializeLine(envelope map[string]json.RawMessage, params map[string]json.RawMessage, line []byte) ([]byte, error) {
	rawCapabilities, ok := params["clientCapabilities"]
	if !ok {
		return append([]byte(nil), line...), nil
	}
	capabilities, err := decodeJSONObject(rawCapabilities, "clientCapabilities")
	if err != nil {
		return nil, err
	}
	if rawFS, ok := capabilities["fs"]; ok {
		fs, err := decodeJSONObject(rawFS, "clientCapabilities.fs")
		if err != nil {
			return nil, err
		}
		for _, name := range []string{"readTextFile", "writeTextFile"} {
			if raw, exists := fs[name]; exists && !isJSONBool(raw) {
				return nil, fmt.Errorf("invalid ACP initialize request: clientCapabilities.fs.%s must be a boolean", name)
			}
		}
		fs["readTextFile"] = json.RawMessage("false")
		fs["writeTextFile"] = json.RawMessage("false")
		encodedFS, err := json.Marshal(fs)
		if err != nil {
			return nil, fmt.Errorf("encoding ACP initialize filesystem capabilities: %w", err)
		}
		capabilities["fs"] = encodedFS
	}
	if rawTerminal, ok := capabilities["terminal"]; ok {
		if !isJSONBool(rawTerminal) {
			return nil, errors.New("invalid ACP initialize request: clientCapabilities.terminal must be a boolean")
		}
		capabilities["terminal"] = json.RawMessage("false")
	}
	encodedCapabilities, err := json.Marshal(capabilities)
	if err != nil {
		return nil, fmt.Errorf("encoding ACP initialize capabilities: %w", err)
	}
	params["clientCapabilities"] = encodedCapabilities
	encodedParams, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("encoding ACP initialize params: %w", err)
	}
	envelope["params"] = encodedParams
	mapped, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encoding ACP initialize request: %w", err)
	}
	if len(mapped) > maxACPLineBytes {
		return nil, fmt.Errorf("mapped ACP input line exceeds %d bytes", maxACPLineBytes)
	}
	return mapped, nil
}

func decodeJSONObject(raw json.RawMessage, field string) (map[string]json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || trimmed[0] != '{' {
		return nil, fmt.Errorf("invalid ACP initialize request: %s must be an object", field)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &object); err != nil || object == nil {
		return nil, fmt.Errorf("invalid ACP initialize request: %s must be an object", field)
	}
	return object, nil
}

func isJSONBool(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return bytes.Equal(trimmed, []byte("true")) || bytes.Equal(trimmed, []byte("false"))
}

// validateJSONDocument rejects invalid UTF-8, duplicate keys (including keys
// whose escaped spellings decode to the same string), trailing values, and
// pathological nesting before encoding/json maps the envelope.
func validateJSONDocument(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := validateJSONValue(decoder, 0); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values (unexpected %v)", token)
		}
		return fmt.Errorf("trailing JSON data: %w", err)
	}
	return nil
}

func validateJSONValue(decoder *json.Decoder, depth int) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch delimiter := token.(type) {
	case json.Delim:
		switch delimiter {
		case '{':
			if depth >= maxACPJSONDepth {
				return fmt.Errorf("JSON nesting exceeds %d levels", maxACPJSONDepth)
			}
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return errors.New("JSON object key is not a string")
				}
				if _, exists := seen[key]; exists {
					return fmt.Errorf("duplicate JSON object key %q", key)
				}
				seen[key] = struct{}{}
				if err := validateJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil {
				return err
			}
			if end != json.Delim('}') {
				return fmt.Errorf("JSON object ended with %v", end)
			}
		case '[':
			if depth >= maxACPJSONDepth {
				return fmt.Errorf("JSON nesting exceeds %d levels", maxACPJSONDepth)
			}
			for decoder.More() {
				if err := validateJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil {
				return err
			}
			if end != json.Delim(']') {
				return fmt.Errorf("JSON array ended with %v", end)
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
		}
	case string, bool, json.Number, nil:
		return nil
	default:
		return fmt.Errorf("unexpected JSON token %T", token)
	}
	return nil
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
	if !utf8.ValidString(cwd) {
		return errors.New("--acp-cwd must be valid UTF-8")
	}
	if !path.IsAbs(cwd) || path.Clean(cwd) != cwd || bytes.IndexByte([]byte(cwd), 0) >= 0 {
		return fmt.Errorf("--acp-cwd must be a clean absolute POSIX path, got %q", cwd)
	}
	return nil
}
