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
