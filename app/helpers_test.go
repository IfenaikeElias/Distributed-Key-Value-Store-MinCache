package main

import (
	"bytes"
	"net"
	"sync"
	"testing"
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
		DB:       &Store{KVStore: make(map[string]value)},
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
