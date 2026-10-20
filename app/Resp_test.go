package main

import (
	"bufio"
	"net"
	"slices"
	"strings"
	"testing"
)

func TestWriteHelpers(t *testing.T) {
	tests := []struct {
		name  string
		write func(net.Conn)
		want  string
	}{
		{"simple string", func(c net.Conn) { writeStr(c, "OK") }, "+OK\r\n"},
		{"error", func(c net.Conn) { writeErr(c, "boom") }, "-ERR boom\r\n"},
		{"bulk string", func(c net.Conn) { writeBulkStr(c, false, "hey") }, "$3\r\nhey\r\n"},
		{"empty bulk string", func(c net.Conn) { writeBulkStr(c, false, "") }, "$0\r\n\r\n"},
		{"null bulk string", func(c net.Conn) { writeBulkStr(c, true, "") }, "$-1\r\n"},
		{"integer", func(c net.Conn) { writeInt(c, 42) }, ":42\r\n"},
		{"array", func(c net.Conn) { writeArray(c, []string{"a", "bc"}) }, "*2\r\n$1\r\na\r\n$2\r\nbc\r\n"},
		{"empty array", func(c net.Conn) { writeArray(c, []string{}) }, "*0\r\n"},
		{"null array", func(c net.Conn) { writeNullArray(c) }, "*-1\r\n"},
		{"rdb file has no trailing CRLF", func(c net.Conn) { writeRDBFile(c, "RDB") }, "$3\r\nRDB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &fakeConn{}
			tt.write(conn)
			assertEqual(t, conn.String(), tt.want)
		})
	}
}

func TestWriteHelpersIgnoreNilConn(t *testing.T) {
	// none of these may panic when there is no client to reply to
	writeStr(nil, "OK")
	writeErr(nil, "boom")
	writeBulkStr(nil, false, "v")
	writeBulkStr(nil, true, "")
	writeInt(nil, 1)
	writeArray(nil, []string{"a"})
	writeNullArray(nil)
}

func TestParseRESP(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr bool
	}{
		{"command with args", "*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n", []string{"ECHO", "hi"}, false},
		{"empty bulk string", "*1\r\n$0\r\n\r\n", []string{""}, false},
		{"CRLF inside a value", "*1\r\n$4\r\na\r\nb\r\n", []string{"a\r\nb"}, false},
		{"empty array", "*0\r\n", []string{}, false},
		{"inline command is rejected", "PING\r\n", nil, true},
		{"non-bulk element", "*1\r\n:1\r\n", nil, true},
		{"bad array count", "*x\r\n", nil, true},
		{"bad bulk length", "*1\r\n$x\r\n", nil, true},
		{"truncated value", "*1\r\n$5\r\nab", nil, true},
		{"empty input", "", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRESP(bufio.NewReader(strings.NewReader(tt.input)))
			assertEqual(t, err != nil, tt.wantErr)
			if !tt.wantErr && !slices.Equal(got, tt.want) {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseRESPReadsConsecutiveCommands(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(encode("PING") + encode("GET", "k")))

	first, _ := parseRESP(reader)
	second, _ := parseRESP(reader)

	assertEqual(t, strings.Join(first, " "), "PING")
	assertEqual(t, strings.Join(second, " "), "GET k")
}
