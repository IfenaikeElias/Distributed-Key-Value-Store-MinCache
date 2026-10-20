package main

import (
	"strconv"
	"testing"
)

// ---- one reply per command (failed commands used to reply twice) ----

func TestFailedCommandGetsExactlyOneReply(t *testing.T) {
	c := connect(t, newTestServer())

	assertEqual(t, c.do("GET"), "-ERR Get: Expected key parameter only\r\n")
	// with a second error queued up, this would read that error instead of PONG
	assertEqual(t, c.do("PING"), "+PONG\r\n")
}

func TestPingWithArgumentGetsErrorReply(t *testing.T) {
	c := connect(t, newTestServer())

	assertEqual(t, c.do("PING", "x"), "-ERR Ping takes no arguement\r\n")
	assertEqual(t, c.do("PING"), "+PONG\r\n")
}

func TestExecWithFailingCommandMatchesArrayHeader(t *testing.T) {
	c := connect(t, newTestServer())

	assertEqual(t, c.do("MULTI"), "+OK\r\n")
	assertEqual(t, c.do("SET", "z"), "+QUEUED\r\n")
	assertEqual(t, c.do("SET", "k", "v"), "+QUEUED\r\n")
	assertEqual(t, c.do("EXEC"),
		"*2\r\n"+
			"-ERR Set: Expected key value pair with/without optional Parameters: EX or PX\r\n"+
			"+OK\r\n")
	assertEqual(t, c.do("PING"), "+PONG\r\n")
}

// ---- failed writes must not be propagated to replicas ----

func TestFailedWritesAreNotPropagated(t *testing.T) {
	s := newTestServer()
	replica := attachReplica(s)
	c := connect(t, s)

	c.do("SET", "s", "abc")
	c.do("RPUSH", "l", "a")
	c.do("INCR", "s")        // not an integer
	c.do("RPUSH", "s", "x")  // not a list
	c.do("LPUSH", "s", "x")  // not a list
	c.do("SET", "k")         // missing value
	c.do("LPOP", "l", "abc") // bad count
	c.do("INCR", "n")
	waitForReplicaToCatchUp(t, s, replica)

	assertEqual(t, replica.String(), encode("SET", "s", "abc")+encode("RPUSH", "l", "a")+encode("INCR", "n"))
	assertEqual(t, s.replicationOffset(), len(replica.String()))
}

func TestFailedWritesInExecAreNotPropagated(t *testing.T) {
	s := newTestServer()
	replica := attachReplica(s)
	c := connect(t, s)

	c.do("SET", "s", "abc")
	c.do("MULTI")
	c.do("INCR", "s") // not an integer
	c.do("INCR", "n")
	c.do("EXEC")
	waitForReplicaToCatchUp(t, s, replica)

	assertEqual(t, replica.String(), encode("SET", "s", "abc")+encode("INCR", "n"))
}

// ---- BLPOP woken by a push: replicas must get the push before the pop ----

func TestBlpopServedByPushIsPropagatedAfterThePush(t *testing.T) {
	s := newTestServer()
	replica := attachReplica(s)
	waiter := blpopInBackground(s, "k", "0")
	waitForWaiters(t, s, "k", 1)
	c := connect(t, s)

	assertEqual(t, c.do("RPUSH", "k", "a", "b"), ":2\r\n")

	assertEqual(t, receive(t, waiter), "*2\r\n$1\r\nk\r\n$1\r\na\r\n")
	waitForReplicaToCatchUp(t, s, replica)
	assertEqual(t, replica.String(), encode("RPUSH", "k", "a", "b")+encode("LPOP", "k"))
}

func TestBlpopIsWokenByPushInsideExec(t *testing.T) {
	s := newTestServer()
	waiter := blpopInBackground(s, "k", "0")
	waitForWaiters(t, s, "k", 1)
	c := connect(t, s)

	c.do("MULTI")
	c.do("RPUSH", "k", "a")
	c.do("EXEC")

	assertEqual(t, receive(t, waiter), "*2\r\n$1\r\nk\r\n$1\r\na\r\n")
}

// ---- unit tests for the rest of the connection loop ----

func TestCommandNamesAreCaseInsensitive(t *testing.T) {
	c := connect(t, newTestServer())

	assertEqual(t, c.do("set", "k", "v"), "+OK\r\n")
	assertEqual(t, c.do("Get", "k"), "$1\r\nv\r\n")
}

func TestUnknownCommand(t *testing.T) {
	c := connect(t, newTestServer())

	assertEqual(t, c.do("FLY"), "-ERR unknown command\r\n")
}

func TestReplicaRejectsClientWrites(t *testing.T) {
	s := newTestServer()
	s.isMaster = false
	c := connect(t, s)

	assertEqual(t, c.do("SET", "k", "v"), "-ERR Replicas are not allowed to write\r\n")
	assertEqual(t, c.do("GET", "k"), "$-1\r\n")
}

func TestReplicaHandshakeAndAckCountedByWait(t *testing.T) {
	s := newTestServer()
	s.master_replid = "abc"
	c := connect(t, s)

	assertEqual(t, c.do("REPLCONF", "listening-port", "6380"), "+OK\r\n")
	assertEqual(t, c.do("PSYNC", "?", "-1"), "+FULLRESYNC abc 0\r\n")
	// the RDB snapshot follows as "$<len>\r\n<bytes>" with no trailing CRLF
	header, _ := c.reader.ReadString('\n')
	size, _ := strconv.Atoi(header[1 : len(header)-2])
	c.reader.Discard(size)

	// pretend 100 bytes of writes went out; propagating for real would block on the unread pipe
	s.replicasMu.Lock()
	s.master_repl_offset = 100
	s.replicasMu.Unlock()

	writeArray(c.conn, []string{"REPLCONF", "ACK", "100"}) // ACK gets no reply
	c.do("PING")                                           // ACK has been processed once this answers

	// the replica has caught up, so WAIT returns straight away without sending GETACK
	assertEqual(t, call(s.Wait, "1", "100"), ":1\r\n")
}

// ---- LPUSH with several elements must replicate in the same order ----

func TestMultiElementLpushReplicatesInOrder(t *testing.T) {
	master := newTestServer()
	replicaConn := attachReplica(master)
	c := connect(t, master)

	c.do("LPUSH", "l", "a", "b", "c")
	waitForReplicaToCatchUp(t, master, replicaConn)

	replica := replay(replicaConn.String())
	assertEqual(t, call(replica.Lrange, "l", "0", "-1"), call(master.Lrange, "l", "0", "-1"))
}
