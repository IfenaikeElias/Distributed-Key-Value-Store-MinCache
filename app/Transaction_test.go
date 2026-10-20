package main

import "testing"

func TestCommandsInsideMultiAreQueuedUntilExec(t *testing.T) {
	s := newTestServer()
	c := connect(t, s)

	assertEqual(t, c.do("MULTI"), "+OK\r\n")
	assertEqual(t, c.do("SET", "k", "1"), "+QUEUED\r\n")
	assertEqual(t, c.do("INCR", "k"), "+QUEUED\r\n")
	assertEqual(t, keyExists(s, "k"), false) // nothing has run yet

	assertEqual(t, c.do("EXEC"), "*2\r\n+OK\r\n:2\r\n")
	assertEqual(t, c.do("GET", "k"), "$1\r\n2\r\n")
}

func TestEmptyExecReturnsEmptyArray(t *testing.T) {
	c := connect(t, newTestServer())

	c.do("MULTI")
	assertEqual(t, c.do("EXEC"), "*0\r\n")
}

func TestDiscardDropsQueuedCommands(t *testing.T) {
	s := newTestServer()
	c := connect(t, s)

	c.do("MULTI")
	c.do("SET", "k", "1")
	assertEqual(t, c.do("DISCARD"), "+OK\r\n")

	assertEqual(t, keyExists(s, "k"), false)
	assertEqual(t, c.do("EXEC"), "-ERR EXEC without MULTI\r\n")
}

func TestTransactionErrors(t *testing.T) {
	c := connect(t, newTestServer())

	assertEqual(t, c.do("EXEC"), "-ERR EXEC without MULTI\r\n")
	assertEqual(t, c.do("DISCARD"), "-ERR DISCARD without MULTI\r\n")
	assertEqual(t, c.do("MULTI", "x"), "-ERR MULTI: command takes no arguement\r\n")

	c.do("MULTI")
	assertEqual(t, c.do("MULTI"), "-ERR MULTI cannot be called against MULTI\r\n")
}
