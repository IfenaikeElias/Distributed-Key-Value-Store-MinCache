package main

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

// fakeConn is a net.Conn that records everything written to it.
// Only Write is implemented; calling any other net.Conn method panics.
type fakeConn struct {
	net.Conn
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *fakeConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *fakeConn) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// newTestServer returns a master with its own empty store, so tests don't share state.
func newTestServer() *Server {
	return &Server{
		isMaster: true,
		DB:       newStore(),
	}
}

// call runs a command handler on a fresh connection and returns the raw RESP it wrote.
func call(handler CommandHandler, args ...string) string {
	conn := &fakeConn{}
	handler(conn, args...)
	return conn.String()
}

// assertEqual fails the test when got != want, like `assert got == want` in pytest.
func assertEqual[T comparable](t *testing.T, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

// keyExists reports whether key is present in the store.
func keyExists(s *Server, key string) bool {
	s.DB.mutexLock.RLock()
	defer s.DB.mutexLock.RUnlock()
	_, ok := s.DB.KVStore[key]
	return ok
}

// encode builds the RESP array a client (or master) would send for a command.
func encode(args ...string) string {
	conn := &fakeConn{}
	writeArray(conn, args)
	return conn.String()
}

// testClient is a client connected to s through handleConnection, the same loop real clients go through.
type testClient struct {
	t      *testing.T
	conn   net.Conn
	reader *bufio.Reader
}

func connect(t *testing.T, s *Server) *testClient {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	go handleConnection(serverSide, s.commandTable(), s)
	clientSide.SetDeadline(time.Now().Add(2 * time.Second)) // fail instead of hanging forever
	t.Cleanup(func() { clientSide.Close() })
	return &testClient{t: t, conn: clientSide, reader: bufio.NewReader(clientSide)}
}

// do sends one command and returns its raw RESP reply.
func (c *testClient) do(args ...string) string {
	c.t.Helper()
	writeArray(c.conn, args)
	return c.read()
}

// read returns exactly one RESP reply (arrays include all their elements).
func (c *testClient) read() string {
	c.t.Helper()
	line, err := c.reader.ReadString('\n')
	if err != nil {
		c.t.Fatalf("reading reply: %v", err)
	}
	n, _ := strconv.Atoi(line[1 : len(line)-2])
	switch line[0] {
	case '$':
		if n < 0 {
			return line
		}
		data := make([]byte, n+2)
		if _, err := io.ReadFull(c.reader, data); err != nil {
			c.t.Fatalf("reading bulk string: %v", err)
		}
		return line + string(data)
	case '*':
		for range max(n, 0) {
			line += c.read()
		}
	}
	return line
}

// attachReplica registers a fake replica on s and returns it, so tests can see what was propagated.
func attachReplica(s *Server) *fakeConn {
	replica := &fakeConn{}
	s.replicas = append(s.replicas, Replica{conn: replica})
	return replica
}

// blpopInBackground starts a BLPOP on its own goroutine; its reply arrives on the returned channel.
func blpopInBackground(s *Server, key, timeout string) <-chan string {
	reply := make(chan string, 1)
	go func() { reply <- call(s.Blpop, key, timeout) }()
	return reply
}

// waitForWaiters blocks until exactly n clients are queued in BLPOP on key.
func waitForWaiters(t *testing.T, s *Server, key string, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.DB.mutexLock.RLock()
		queued := len(s.DB.waiters[key])
		s.DB.mutexLock.RUnlock()
		if queued == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d BLPOP clients on %q", n, key)
}

// receive waits for a background reply, failing instead of hanging if it never comes.
func receive(t *testing.T, reply <-chan string) string {
	t.Helper()
	select {
	case r := <-reply:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reply")
		return ""
	}
}
