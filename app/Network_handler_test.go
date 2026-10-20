package main

import "testing"

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
	c.do("PING") // replies come before propagation; once PING answers, INCR n has been propagated

	assertEqual(t, replica.String(), encode("SET", "s", "abc")+encode("RPUSH", "l", "a")+encode("INCR", "n"))
	assertEqual(t, replOffset(s), len(replica.String()))
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
	c.do("PING")

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
	c.do("PING") // the loop has finished serving and propagating the RPUSH

	assertEqual(t, receive(t, waiter), "*2\r\n$1\r\nk\r\n$1\r\na\r\n")
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
