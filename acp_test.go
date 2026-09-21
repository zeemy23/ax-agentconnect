// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

type fragmentedReader struct {
	data   []byte
	chunks []int
	offset int
	chunk  int
}

func (r *fragmentedReader) Read(dst []byte) (int, error) {
	if r.offset == len(r.data) {
		return 0, io.EOF
	}
	n := len(dst)
	if r.chunk < len(r.chunks) && r.chunks[r.chunk] < n {
		n = r.chunks[r.chunk]
	}
	if n > len(r.data)-r.offset {
		n = len(r.data) - r.offset
	}
	r.chunk++
	copy(dst, r.data[r.offset:r.offset+n])
	r.offset += n
	return n, nil
}

func decodeACPObject(t *testing.T, line []byte) map[string]json.RawMessage {
	t.Helper()
	line = bytes.TrimSuffix(line, []byte{'\r'})
	var object map[string]json.RawMessage
	if err := json.Unmarshal(line, &object); err != nil {
		t.Fatalf("decoding ACP line %q: %v", line, err)
	}
	return object
}

func TestTransformACPRewritesSessionCWDAndPreservesOtherFrames(t *testing.T) {
	newRequest := `{"jsonrpc":"2.0","id":101,"method":"session/new","params":{"cwd":"client-cwd","nested":{"cwd":"nested-cwd"},"mcpServers":[]}}`
	loadRequest := `{"jsonrpc":"2.0","id":"load-id","method":"session/load","params":{"cwd":"another-client-cwd","mcpServers":[]}}`
	otherRequest := `{"jsonrpc":"2.0","id":"other-id","method":"session/prompt","params":["hello"]}`
	response := `{"jsonrpc":"2.0","id":101,"result":{"sessionId":"session-1"}}`
	input := newRequest + "\r\n" + loadRequest + "\n" + otherRequest + "\n" + response + "\n"
	reader := &fragmentedReader{
		data:   []byte(input),
		chunks: []int{1, 2, 7, 3, 1, 11, 5, 2, 13},
	}
	var output bytes.Buffer
	if err := transformACPInput(reader, "/workspace/work", func(data []byte) error {
		_, err := output.Write(data)
		return err
	}); err != nil {
		t.Fatalf("transformACPInput() error = %v", err)
	}

	lines := bytes.Split(output.Bytes(), []byte{'\n'})
	if len(lines) != 5 || len(lines[4]) != 0 {
		t.Fatalf("output lines = %d, want four terminated lines: %q", len(lines), output.Bytes())
	}
	newObject := decodeACPObject(t, lines[0])
	loadObject := decodeACPObject(t, lines[1])
	for name, object := range map[string]map[string]json.RawMessage{"session/new": newObject, "session/load": loadObject} {
		var method string
		if err := json.Unmarshal(object["method"], &method); err != nil || method != name {
			t.Fatalf("%s method = %q, want %q", name, method, name)
		}
		var params map[string]json.RawMessage
		if err := json.Unmarshal(object["params"], &params); err != nil {
			t.Fatalf("%s params: %v", name, err)
		}
		var cwd string
		if err := json.Unmarshal(params["cwd"], &cwd); err != nil || cwd != "/workspace/work" {
			t.Fatalf("%s cwd = %q, want operator cwd", name, cwd)
		}
		var servers []json.RawMessage
		if err := json.Unmarshal(params["mcpServers"], &servers); err != nil || len(servers) != 0 {
			t.Fatalf("%s mcpServers = %s, want empty array", name, params["mcpServers"])
		}
	}
	if got := string(newObject["id"]); got != "101" {
		t.Fatalf("session/new id = %s, want 101", got)
	}
	if got := string(loadObject["id"]); got != `"load-id"` {
		t.Fatalf("session/load id = %s, want load-id", got)
	}
	if got := string(bytes.TrimSuffix(lines[2], []byte{'\r'})); got != otherRequest {
		t.Fatalf("other request changed: %s", got)
	}
	if got := string(bytes.TrimSuffix(lines[3], []byte{'\r'})); got != response {
		t.Fatalf("response changed: %s", got)
	}
}

func TestTransformACPPreservesHarnessCapabilities(t *testing.T) {
	initialize := `{"jsonrpc":"2.0","id":"initialize","method":"initialize","params":{"clientCapabilities":{"fs":{"readTextFile":true},"terminal":true},"clientInfo":{"name":"future-harness","version":"1"}}}` + "\n"
	var output bytes.Buffer
	if err := transformACPInput(strings.NewReader(initialize), "/workspace/work", func(data []byte) error {
		_, err := output.Write(data)
		return err
	}); err != nil {
		t.Fatalf("transformACPInput() error = %v", err)
	}
	if got := output.String(); got != initialize {
		t.Fatalf("initialize frame changed:\n got: %s\nwant: %s", got, initialize)
	}
}

func TestTransformACPRejectsMalformedAndOversizedFrames(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "invalid json", input: "{"},
		{name: "non-object", input: "[]\n"},
		{name: "wrong version", input: `{"jsonrpc":"1.0","id":1,"method":"x"}`},
		{name: "missing response fields", input: `{"jsonrpc":"2.0","id":1}`},
		{name: "scalar params", input: `{"jsonrpc":"2.0","id":1,"method":"x","params":true}`},
		{name: "response both result and error", input: `{"jsonrpc":"2.0","id":1,"result":null,"error":{}}`},
		{name: "response missing id", input: `{"jsonrpc":"2.0","result":null}`},
		{name: "oversized", input: strings.Repeat("x", maxACPLineBytes+1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := transformACPInput(strings.NewReader(tt.input), "/workspace/work", func([]byte) error { return nil })
			if err == nil {
				t.Fatal("transformACPInput() succeeded for malformed input")
			}
		})
	}
}

func TestTransformACPRejectsNonEmptyMCPServers(t *testing.T) {
	for _, method := range []string{"session/new", "session/load", "session/prompt"} {
		t.Run(method, func(t *testing.T) {
			input := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":{"mcpServers":[{"name":"ignored"}]}}` + "\n"
			err := transformACPInput(strings.NewReader(input), "/workspace/work", func([]byte) error { return nil })
			if err == nil || !strings.Contains(err.Error(), "mcpServers") {
				t.Fatalf("error = %v, want mcpServers rejection", err)
			}
		})
	}
}

func TestValidateACPCWD(t *testing.T) {
	if err := validateACPCWD(""); err != nil {
		t.Fatalf("empty cwd rejected: %v", err)
	}
	if err := validateACPCWD("workspace/work"); err == nil {
		t.Fatal("relative cwd accepted")
	}
	if err := validateACPCWD("/workspace/work"); err != nil {
		t.Fatalf("absolute cwd rejected: %v", err)
	}
}
