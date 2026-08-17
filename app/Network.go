package main

import (
	"fmt"
	"net"
	"os"
)

// ------ utils for establishing connections ------

func (s *Server) ListenForConn() {
	/*
		func to listen for connections and handle error
	*/
	port := "0.0.0.0:" + s.port
	ln, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("Error starting server:", err.Error())
		os.Exit(1)
	}
	s.Listener = ln
}

func (s *Server) AcceptConn() (net.Conn, error) {
	/*
		func to accept connections and handle error
	*/
	conn, err := s.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return conn, nil
}