package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

//  ---- utils for writing RESP responses -----

func writeStr(conn net.Conn, s string) {
	if conn == nil {
		return
	}
	resp := "+" + s + CRLF
	conn.Write([]byte(resp))
}

func writeErr(conn net.Conn, msg string) {
	if conn == nil {
		return
	}
	conn.Write([]byte("-ERR " + msg + CRLF))
}

func writeBulkStr(conn net.Conn, isNull bool, s string) {
	if conn == nil {
		return
	}
	if isNull {
		conn.Write([]byte("$-1" + CRLF))
	} else {
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
		conn.Write([]byte(resp))
	}
}

func writeInt(conn net.Conn, n int) {
	if conn == nil {
		return
	}
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))
}

func writeArray(conn net.Conn, items []string) {
	if conn == nil {
		return
	}
	var resp strings.Builder
	fmt.Fprintf(&resp, "*%d\r\n", len(items))
	for _, items := range items {
		fmt.Fprintf(&resp, "$%d\r\n%s\r\n", len(items), items)
	}
	conn.Write([]byte(resp.String()))
}

func writeNullArray(conn net.Conn) {
	if conn == nil {
		return
	}
	conn.Write([]byte("*-1\r\n"))
}

func writeRDBFile(conn net.Conn, content string) {
    fileContent := fmt.Sprintf("$%d\r\n%s", len(content), content)
    conn.Write([]byte(fileContent))
}

// ----- Standard RESP Parser ---------

func parseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("parseRESP: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")

	// must start with * (array)
	if line == "" || line[0] != '*' {
		return nil, fmt.Errorf("ERROR: expected array")
	}

	var count int
	if _, err := fmt.Sscanf(line[1:], "%d", &count); err !=  nil {
		return nil, fmt.Errorf("bad array count %w", err)
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		// read $<len>
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("parseRESP reading length: %w", err)
		}
		// check if the resp array is bad, i.e has alphabet in place of length
		lenLine = strings.TrimRight(lenLine, "\r\n")
		if lenLine == "" || lenLine[0] != '$' {
			return nil, fmt.Errorf("parseRESP: expected bulk string, got %q", lenLine)
		}
		// how many byte of string to read as stated by lenLine e.g $3
		n, err := strconv.Atoi(lenLine[1:])
		if err !=  nil {
			return nil, fmt.Errorf("bad bulk lenght: %w", err)
		}
		// read the actual value
		buff := make([]byte, n + 2) // + 2 to accomodate the extra "\r\n"
		if _, err :=  io.ReadFull(reader, buff); err != nil {
			return nil, fmt.Errorf("parseRESP reading value: %w", err)
		}
		args = append(args, string(buff[:n]))
	}
	return args, nil
}