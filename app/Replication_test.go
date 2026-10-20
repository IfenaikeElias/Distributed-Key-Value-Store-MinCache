package main

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
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

func TestReplicaAppliesPropagatedLpop(t *testing.T) {
	s := newTestServer()
	s.isMaster = false

	// what the master sends when a BLPOP client is served by a push
	feedFromMaster(s, []string{"RPUSH", "k", "a", "b"}, []string{"LPOP", "k"})

	assertEqual(t, call(s.Lrange, "k", "0", "-1"), "*1\r\n$1\r\nb\r\n")
}

// ---- unit tests for the rest of Replication.go ----

func TestRespSizeMatchesEncodedLength(t *testing.T) {
	for _, cmd := range [][]string{{"PING"}, {"SET", "key", "value"}, {"REPLCONF", "GETACK", "*"}, {"SET", "k", ""}} {
		assertEqual(t, respSize(cmd), len(encode(cmd...)))
	}
}

func TestGenerateReplicationID(t *testing.T) {
	id := GenerateReplicationID()

	assertEqual(t, len(id), 40)
	if strings.Trim(id, "0123456789abcdef") != "" {
		t.Errorf("id %q is not lowercase hex", id)
	}
	if id == GenerateReplicationID() {
		t.Error("two generated ids are equal")
	}
}

func TestPropagateToReplicasSendsToAllAndAdvancesOffset(t *testing.T) {
	s := newTestServer()
	first, second := attachReplica(s), attachReplica(s)

	s.propagateToReplicas([]string{"SET", "k", "v"})

	assertEqual(t, first.String(), encode("SET", "k", "v"))
	assertEqual(t, second.String(), encode("SET", "k", "v"))
	assertEqual(t, s.master_repl_offset, len(encode("SET", "k", "v")))
}

func TestReplicaAnswersGetackWithBytesProcessedSoFar(t *testing.T) {
	s := newTestServer()
	s.isMaster = false

	masterConn := feedFromMaster(s,
		[]string{"SET", "k", "v"},
		[]string{"REPLCONF", "GETACK", "*"},
	)

	// the offset in the ACK counts everything before the GETACK itself
	offset := strconv.Itoa(len(encode("SET", "k", "v")))
	assertEqual(t, masterConn.String(), encode("REPLCONF", "ACK", offset))
}

// ---- master_repl_offset must only be read under replicasMu (run with -race) ----

func TestOffsetReadsAreSafeDuringPropagation(t *testing.T) {
	s := newTestServer()
	attachReplica(s)

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 200 {
			s.propagateToReplicas([]string{"SET", "k", "v"})
		}
	})
	for range 200 {
		call(s.Info)
		call(s.Wait, "0", "0")
	}
	wg.Wait()

	assertEqual(t, s.replicationOffset(), 200*len(encode("SET", "k", "v")))
}

// ---- connecting to the master ----

// fakeMaster accepts one replica and answers each command it sends with the next scripted reply.
// After the script runs out it sends `after` (e.g. propagated writes) and hangs up.
func fakeMaster(t *testing.T, replies []string, after string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for _, reply := range replies {
			if _, err := parseRESP(reader); err != nil {
				return
			}
			conn.Write([]byte(reply))
		}
		conn.Write([]byte(after))
		// hang up cleanly: the replica reads EOF, and we drain whatever it still sends until it closes
		conn.(*net.TCPConn).CloseWrite()
		io.Copy(io.Discard, reader)
	}()
	return ln.Addr().String()
}

func newTestReplica(masterAddr string) *Server {
	s := newTestServer()
	s.isMaster = false
	s.port = "6380"
	s.replicaOf = masterAddr
	return s
}

func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("error = %v, want it to contain %q", err, want)
	}
}

func TestConnectToMasterSyncsAndAppliesWrites(t *testing.T) {
	addr := fakeMaster(t,
		[]string{"+PONG\r\n", "+OK\r\n", "+OK\r\n", "+FULLRESYNC abc 0\r\n$3\r\nRDB"},
		encode("SET", "k", "v"))
	s := newTestReplica(addr)

	// returns once the fake master hangs up after sending the SET
	if err := s.connectToMaster(); err != nil {
		t.Fatalf("connectToMaster: %v", err)
	}
	assertEqual(t, call(s.Get, "k"), "$1\r\nv\r\n")
}

func TestConnectToMasterReportsUnreachableMaster(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close() // nothing is listening here any more

	assertErrorContains(t, newTestReplica(addr).connectToMaster(), "connection refused")
}

func TestConnectToMasterReportsHandshakeFailures(t *testing.T) {
	tests := []struct {
		name    string
		replies []string
		want    string
	}{
		{"PING rejected", []string{"-ERR nope\r\n"}, `got "-ERR nope\r\n"`},
		{"master hangs up after PING", []string{}, "stage 1 (PING) failed: EOF"},
		{"master hangs up after REPLCONF port", []string{"+PONG\r\n"}, "stage 2 (REPLCONF listening-port) failed: EOF"},
		{"master hangs up after REPLCONF capa", []string{"+PONG\r\n", "+OK\r\n"}, "stage 3 (REPLCONF capa) failed: EOF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestReplica(fakeMaster(t, tt.replies, ""))
			assertErrorContains(t, s.connectToMaster(), tt.want)
		})
	}
}
