package main

import "testing"

// ---- LPOP on an emptied list (used to panic: index out of range) ----

func TestLpopOnEmptiedListReturnsNull(t *testing.T) {
	s := newTestServer()
	call(s.Rpush, "k", "a")
	call(s.Lpop, "k")

	assertEqual(t, call(s.Lpop, "k"), "$-1\r\n")
}

func TestLpopDeletesKeyWhenListEmpties(t *testing.T) {
	s := newTestServer()
	call(s.Rpush, "k", "a", "b")

	call(s.Lpop, "k")
	assertEqual(t, keyExists(s, "k"), true)

	call(s.Lpop, "k")
	assertEqual(t, keyExists(s, "k"), false)
}

func TestLpopWithCountDeletesKeyWhenListEmpties(t *testing.T) {
	s := newTestServer()
	call(s.Rpush, "k", "a", "b")

	assertEqual(t, call(s.Lpop, "k", "5"), "*2\r\n$1\r\na\r\n$1\r\nb\r\n")
	assertEqual(t, keyExists(s, "k"), false)
}

func TestBlpopDeletesKeyWhenListEmpties(t *testing.T) {
	s := newTestServer()
	call(s.Rpush, "k", "a")

	assertEqual(t, call(s.Blpop, "k", "1"), "*2\r\n$1\r\nk\r\n$1\r\na\r\n")
	assertEqual(t, keyExists(s, "k"), false)
}
