package main

import (
	"bufio"
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
