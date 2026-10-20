package main

import (
	"net"
	"testing"
)

func TestListenAndAcceptConn(t *testing.T) {
	s := newTestServer()
	s.port = "0" // let the OS pick a free port
	s.ListenForConn()
	defer s.Listener.Close()

	client, err := net.Dial("tcp", s.Listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	conn, err := s.AcceptConn()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	conn.Close()
}
