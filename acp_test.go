// Copyright 2026 ax-agentconnect contributors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"
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

func TestTransformACPSanitizesBridgeCapabilities(t *testing.T) {
	initialize := `{"jsonrpc":"2.0","id":"initialize","method":"initialize","params":{"clientCapabilities":{"fs":{"readTextFile":true,"writeTextFile":true,"_meta":{"future":{"enabled":true}}},"terminal":true,"auth":{"terminal":true},"elicitation":{"form":{}},"futureExtension":{"enabled":true}},"clientInfo":{"name":"future-harness","version":"1"}}}` + "\n"
	var output bytes.Buffer
	if err := transformACPInput(strings.NewReader(initialize), "/workspace/work", func(data []byte) error {
		_, err := output.Write(data)
		return err
	}); err != nil {
		t.Fatalf("transformACPInput() error = %v", err)
	}
	line := bytes.TrimSuffix(output.Bytes(), []byte{'\n'})
	object := decodeACPObject(t, line)
	var params map[string]json.RawMessage
	if err := json.Unmarshal(object["params"], &params); err != nil {
		t.Fatalf("initialize params: %v", err)
	}
	var capabilities map[string]json.RawMessage
	if err := json.Unmarshal(params["clientCapabilities"], &capabilities); err != nil {
		t.Fatalf("client capabilities: %v", err)
	}
	var fs map[string]json.RawMessage
	if err := json.Unmarshal(capabilities["fs"], &fs); err != nil {
		t.Fatalf("filesystem capabilities: %v", err)
	}
	for _, name := range []string{"readTextFile", "writeTextFile"} {
		var enabled bool
		if err := json.Unmarshal(fs[name], &enabled); err != nil || enabled {
			t.Fatalf("fs.%s = %s, want false", name, fs[name])
		}
	}
	var terminal bool
	if err := json.Unmarshal(capabilities["terminal"], &terminal); err != nil || terminal {
		t.Fatalf("terminal = %s, want false", capabilities["terminal"])
	}
	for _, name := range []string{"auth", "elicitation", "futureExtension"} {
		if _, ok := capabilities[name]; !ok {
			t.Fatalf("unrelated capability %q was dropped", name)
		}
	}
	if got := string(fs["_meta"]); got != `{"future":{"enabled":true}}` {
		t.Fatalf("fs extension metadata changed: %s", got)
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
		{name: "duplicate key", input: `{"jsonrpc":"2.0","jsonrpc":"2.0","id":1,"method":"x","params":{}}`},
		{name: "duplicate escaped key", input: `{"jsonrpc":"2.0","id":1,"method":"x","params":{"a":1,"\u0061":2}}`},
		{name: "invalid utf8", input: string([]byte{'{', '"', 'x', '"', ':', 0xff, '}'})},
		{name: "too deeply nested", input: strings.Repeat("[", maxACPJSONDepth+1) + strings.Repeat("]", maxACPJSONDepth+1)},
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

func TestTransformACPRejectsMalformedControlEnvelopes(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "initialize missing params", input: `{"jsonrpc":"2.0","id":1,"method":"initialize"}`},
		{name: "initialize array params", input: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":[]}`},
		{name: "initialize null capabilities", input: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientCapabilities":null}}`},
		{name: "initialize null fs", input: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientCapabilities":{"fs":null}}}`},
		{name: "initialize nonboolean terminal", input: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientCapabilities":{"terminal":null}}}`},
		{name: "session new missing params", input: `{"jsonrpc":"2.0","id":1,"method":"session/new"}`},
		{name: "session load null params", input: `{"jsonrpc":"2.0","id":1,"method":"session/load","params":null}`},
		{name: "null mcp servers", input: `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"mcpServers":null}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := transformACPInput(strings.NewReader(tt.input), "/workspace/work", func([]byte) error { return nil }); err == nil {
				t.Fatal("transformACPInput() accepted malformed control envelope")
			}
		})
	}
}

type errorReader struct {
	data []byte
	err  error
	done bool
}

func (r *errorReader) Read(dst []byte) (int, error) {
	if r.done {
		return 0, r.err
	}
	r.done = true
	n := copy(dst, r.data)
	return n, r.err
}

func TestTransformACPDoesNotEmitReadErrorPrefix(t *testing.T) {
	complete := []byte(`{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{}}` + "\n")
	partial := []byte(`{"jsonrpc":"2.0","id":2,"method":"session/prompt","params":{"text":`)
	reader := &errorReader{data: append(complete, partial...), err: errors.New("input disconnected")}
	var output bytes.Buffer
	err := transformACPInput(reader, "/workspace/work", func(data []byte) error {
		_, writeErr := output.Write(data)
		return writeErr
	})
	if err == nil || !strings.Contains(err.Error(), "input disconnected") {
		t.Fatalf("transformACPInput() error = %v, want input disconnect", err)
	}
	if got, want := output.String(), string(complete); got != want {
		t.Fatalf("output = %q, want only complete frame %q", got, want)
	}
}

func TestTransformACPRejectsUnterminatedFrames(t *testing.T) {
	input := `{"jsonrpc":"2.0","id":1,"method":"session/prompt","params":{}}`
	var inputOutput bytes.Buffer
	err := transformACPInput(strings.NewReader(input), "/workspace/work", func(data []byte) error {
		_, writeErr := inputOutput.Write(data)
		return writeErr
	})
	if err == nil || !strings.Contains(err.Error(), "unterminated NDJSON frame") {
		t.Fatalf("transformACPInput() error = %v, want unterminated-frame rejection", err)
	}
	if inputOutput.Len() != 0 {
		t.Fatalf("unterminated input frame was emitted: %q", inputOutput.String())
	}

	var outputOutput bytes.Buffer
	err = readACPOutputForTest(strings.NewReader(input), func(data []byte) error {
		_, writeErr := outputOutput.Write(data)
		return writeErr
	})
	if err == nil || !strings.Contains(err.Error(), "complete newline-delimited frame") {
		t.Fatalf("readACPOutputForTest() error = %v, want unterminated-frame rejection", err)
	}
	if outputOutput.Len() != 0 {
		t.Fatalf("unterminated output frame was emitted: %q", outputOutput.String())
	}
}

func TestTransformACPOutputRejectsUnsupportedClientRequests(t *testing.T) {
	for _, method := range []string{"fs/read_text_file", "fs/write_text_file", "terminal/create", "terminal/output"} {
		t.Run(method, func(t *testing.T) {
			input := `{"jsonrpc":"2.0","id":1,"method":"` + method + `","params":{}}` + "\n"
			var output bytes.Buffer
			err := readACPOutputForTest(strings.NewReader(input), func(data []byte) error {
				_, writeErr := output.Write(data)
				return writeErr
			})
			if err == nil || !strings.Contains(err.Error(), "unsupported client method") {
				t.Fatalf("error = %v, want unsupported-client-method rejection", err)
			}
			if output.Len() != 0 {
				t.Fatalf("unsupported request was emitted: %q", output.String())
			}
		})
	}
}

func TestTransformACPOutputRejectsMalformedEnvelopes(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
	}{
		{name: "missing version", input: `{"id":1,"method":"session/update","params":{}}`},
		{name: "wrong version", input: `{"jsonrpc":"1.0","id":1,"method":"session/update","params":{}}`},
		{name: "non-object", input: `[]`},
		{name: "response missing id", input: `{"jsonrpc":"2.0","result":null}`},
		{name: "response missing result or error", input: `{"jsonrpc":"2.0","id":1}`},
		{name: "response both result and error", input: `{"jsonrpc":"2.0","id":1,"result":null,"error":{}}`},
		{name: "request with result", input: `{"jsonrpc":"2.0","id":1,"method":"session/update","result":null}`},
		{name: "boolean request id", input: `{"jsonrpc":"2.0","id":true,"method":"session/update"}`},
		{name: "scalar params", input: `{"jsonrpc":"2.0","method":"session/update","params":true}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer
			err := readACPOutputForTest(strings.NewReader(tt.input+"\n"), func(data []byte) error {
				_, writeErr := output.Write(data)
				return writeErr
			})
			if err == nil {
				t.Fatal("readACPOutputForTest() accepted malformed envelope")
			}
			if output.Len() != 0 {
				t.Fatalf("malformed output frame was emitted: %q", output.String())
			}
		})
	}
}

func TestTransformACPOutputPreservesAllowedFrames(t *testing.T) {
	input := `{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"s","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"ok"}}}}` + "\r\n" +
		`{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s"}}` + "\n"
	reader := &fragmentedReader{data: []byte(input), chunks: []int{1, 3, 2, 11, 5, 7, 13}}
	var output bytes.Buffer
	if err := readACPOutputForTest(reader, func(data []byte) error {
		_, writeErr := output.Write(data)
		return writeErr
	}); err != nil {
		t.Fatalf("readACPOutputForTest() error = %v", err)
	}
	if got := output.String(); got != input {
		t.Fatalf("output changed allowed frames:\n got: %q\nwant: %q", got, input)
	}
}

func TestTransformACPOutputDoesNotEmitReadErrorPrefix(t *testing.T) {
	complete := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{}}` + "\n")
	partial := []byte(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":`)
	reader := &errorReader{data: append(complete, partial...), err: errors.New("output disconnected")}
	var output bytes.Buffer
	err := readACPOutputForTest(reader, func(data []byte) error {
		_, writeErr := output.Write(data)
		return writeErr
	})
	if err == nil || !strings.Contains(err.Error(), "output disconnected") {
		t.Fatalf("readACPOutputForTest() error = %v, want output disconnect", err)
	}
	if got, want := output.String(), string(complete); got != want {
		t.Fatalf("output = %q, want only complete frame %q", got, want)
	}
}

func TestTransformACPRejectsOversizedRewrite(t *testing.T) {
	input := `{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}` + "\n"
	cwd := "/" + strings.Repeat("x", maxACPLineBytes)
	var emitted bool
	err := transformACPInput(strings.NewReader(input), cwd, func([]byte) error {
		emitted = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "mapped ACP input line exceeds") {
		t.Fatalf("error = %v, want mapped-line bound", err)
	}
	if emitted {
		t.Fatal("oversized mapped frame was emitted")
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
	for _, cwd := range []string{"//workspace/work", "/workspace/./work", "/workspace/../work", "/workspace/work/", `C:\\workspace\\work`, "/workspace/\x00work"} {
		t.Run(cwd, func(t *testing.T) {
			if err := validateACPCWD(cwd); err == nil {
				t.Fatalf("unclean cwd %q accepted", cwd)
			}
		})
	}
	if err := validateACPCWD("/"); err != nil {
		t.Fatalf("root cwd rejected: %v", err)
	}
}

func FuzzMapACPLine(f *testing.F) {
	for _, seed := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientCapabilities":{"fs":{"readTextFile":true},"terminal":true,"x":{"enabled":true}}}}`,
		`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"client","mcpServers":[]}}`,
		`{"jsonrpc":"2.0","id":1,"result":null}`,
		`{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"nested":{"cwd":"client"}}}`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		mapped, err := mapACPLine([]byte(line), "/workspace/work")
		if err != nil {
			return
		}
		if len(mapped) > maxACPLineBytes {
			t.Fatalf("mapped line length %d exceeds %d", len(mapped), maxACPLineBytes)
		}
		if !utf8.Valid(mapped) {
			t.Fatal("mapped line is invalid UTF-8")
		}
		if err := validateJSONDocument(mapped); err != nil {
			t.Fatalf("mapped line is not a validated JSON document: %v", err)
		}
	})
}

// Exercise the same incremental output validator used by the gRPC receiver.
func readACPOutputForTest(r io.Reader, emit func([]byte) error) error {
	var frames outputFrameValidator
	buffer := make([]byte, 64<<10)
	for {
		n, err := r.Read(buffer)
		if frameErr := frames.feed(buffer[:n], emit); frameErr != nil {
			return frameErr
		}
		if errors.Is(err, io.EOF) {
			return frames.flush()
		}
		if err != nil {
			return err
		}
	}
}
