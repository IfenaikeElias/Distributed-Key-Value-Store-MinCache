package main

import (
	"bufio"
	"strings"
	"testing"
)

// feedFromMaster runs the replica's apply loop over the given commands, as if the master sent them.
// listenToMaster returns once the stream hits EOF.
func feedFromMaster(s *Server, commands ...[]string) *fakeConn {
	var stream strings.Builder
	for _, cmd := range commands {
		stream.WriteString(encode(cmd...))
	}
	masterConn := &fakeConn{}
	s.listenToMaster(bufio.NewReader(strings.NewReader(stream.String())), masterConn)
	return masterConn
}

// ---- replica applying writes from the master (used to panic: nil conn) ----

func TestReplicaAppliesPropagatedWrites(t *testing.T) {
	s := newTestServer()
	s.isMaster = false

	feedFromMaster(s,
		[]string{"SET", "a", "1"},
		[]string{"INCR", "n"},
		[]string{"INCR", "n"},
		[]string{"RPUSH", "l", "x", "y"},
		[]string{"LPUSH", "l", "w"},
	)

	assertEqual(t, call(s.Get, "a"), "$1\r\n1\r\n")
	assertEqual(t, call(s.Get, "n"), "$1\r\n2\r\n")
	assertEqual(t, call(s.Lrange, "l", "0", "-1"), "*3\r\n$1\r\nw\r\n$1\r\nx\r\n$1\r\ny\r\n")
}

func TestReplicaSendsNoRepliesForPropagatedWrites(t *testing.T) {
	s := newTestServer()
	s.isMaster = false

	masterConn := feedFromMaster(s, []string{"SET", "a", "1"}, []string{"INCR", "a"})

	assertEqual(t, masterConn.String(), "")
}
