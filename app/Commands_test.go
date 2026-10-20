package main

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

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

// ---- concurrent INCR (used to lose updates: lock released between read and write) ----

func TestConcurrentIncrLosesNoUpdates(t *testing.T) {
	s := newTestServer()
	const clients, incrsEach = 50, 200

	var wg sync.WaitGroup
	for range clients {
		wg.Go(func() {
			for range incrsEach {
				s.Incr(nil, "n")
			}
		})
	}
	wg.Wait()

	assertEqual(t, call(s.Get, "n"), "$5\r\n10000\r\n")
}

// ---- BLPOP waits on a per-key channel queue (used to busy-wait on the lock) ----

func TestBlpopTimesOutWithNullArray(t *testing.T) {
	s := newTestServer()

	assertEqual(t, call(s.Blpop, "k", "0.05"), "*-1\r\n")
	waitForWaiters(t, s, "k", 0) // gave up its place in the queue
}

func TestBlpopOnNonListKeyReturnsError(t *testing.T) {
	s := newTestServer()
	call(s.Set, "k", "v")

	assertEqual(t, call(s.Blpop, "k", "0"), "-ERR BLPOP: command got a non list_key arguement\r\n")
}

func TestBlpopIsWokenByPush(t *testing.T) {
	s := newTestServer()
	reply := blpopInBackground(s, "k", "0")
	waitForWaiters(t, s, "k", 1)

	call(s.Rpush, "k", "a")
	s.serveBlockedClients("k")

	assertEqual(t, receive(t, reply), "*2\r\n$1\r\nk\r\n$1\r\na\r\n")
	assertEqual(t, keyExists(s, "k"), false)
}

func TestBlpopServesLongestWaitingClientFirst(t *testing.T) {
	s := newTestServer()
	first := blpopInBackground(s, "k", "0")
	waitForWaiters(t, s, "k", 1)
	second := blpopInBackground(s, "k", "0")
	waitForWaiters(t, s, "k", 2)

	call(s.Rpush, "k", "a", "b")
	s.serveBlockedClients("k")

	assertEqual(t, receive(t, first), "*2\r\n$1\r\nk\r\n$1\r\na\r\n")
	assertEqual(t, receive(t, second), "*2\r\n$1\r\nk\r\n$1\r\nb\r\n")
}

func TestBlpopKeepsItemHandedOverAsItTimesOut(t *testing.T) {
	s := newTestServer()
	reply := blpopInBackground(s, "k", "0.02")
	waitForWaiters(t, s, "k", 1)

	// hold the lock past the timeout, so the waiter has timed out but can't leave the queue yet
	s.DB.mutexLock.Lock()
	time.Sleep(50 * time.Millisecond)
	// then hand it an item exactly like serveBlockedClients does
	ch := s.DB.waiters["k"][0]
	s.DB.removeWaiter("k", ch)
	ch <- "a"
	s.DB.mutexLock.Unlock()

	// it must reply with the item, not null; otherwise "a" would be lost
	assertEqual(t, receive(t, reply), "*2\r\n$1\r\nk\r\n$1\r\na\r\n")
}

func TestBlpopUnderLoadLosesNothing(t *testing.T) {
	s := newTestServer()
	const pushes = 500

	// stress: tiny timeouts racing pushes, many BLPOPs time out while others get items
	replies := make([]<-chan string, pushes)
	for i := range pushes {
		replies[i] = blpopInBackground(s, "k", "0.001")
		call(s.Rpush, "k", strconv.Itoa(i))
		s.serveBlockedClients("k")
	}

	// every pushed item must end up either delivered to a client or still in the list, exactly once
	seen := map[string]int{}
	for _, reply := range replies {
		if r := receive(t, reply); r != "*-1\r\n" {
			lines := strings.Split(r, "\r\n")
			seen[lines[len(lines)-2]]++
		}
	}
	s.DB.mutexLock.RLock()
	for _, item := range s.DB.KVStore["k"].listVal {
		seen[item]++
	}
	s.DB.mutexLock.RUnlock()

	assertEqual(t, len(seen), pushes)
	for item, count := range seen {
		if count != 1 {
			t.Errorf("item %s seen %d times, want 1", item, count)
		}
	}
}

// ---- unit tests for the rest of the command handlers ----

func TestIsWriteCommand(t *testing.T) {
	for _, cmd := range []string{"SET", "RPUSH", "LPUSH", "LPOP", "INCR"} {
		assertEqual(t, isWriteCommand(cmd), true)
	}
	for _, cmd := range []string{"GET", "PING", "LRANGE", "LLEN", "BLPOP", "WAIT"} {
		assertEqual(t, isWriteCommand(cmd), false)
	}
}

func TestPingAndEcho(t *testing.T) {
	assertEqual(t, call(Ping), "+PONG\r\n")
	assertEqual(t, call(Echo, "hello"), "$5\r\nhello\r\n")
}

func TestSetAndGet(t *testing.T) {
	s := newTestServer()

	assertEqual(t, call(s.Get, "k"), "$-1\r\n")
	assertEqual(t, call(s.Set, "k", "v"), "+OK\r\n")
	assertEqual(t, call(s.Get, "k"), "$1\r\nv\r\n")
	assertEqual(t, call(s.Set, "k", "w"), "+OK\r\n") // overwrite
	assertEqual(t, call(s.Get, "k"), "$1\r\nw\r\n")
}

func TestSetWithExpiry(t *testing.T) {
	s := newTestServer()

	call(s.Set, "px", "v", "PX", "30")
	call(s.Set, "ex", "v", "ex", "100") // option is case-insensitive
	assertEqual(t, call(s.Get, "px"), "$1\r\nv\r\n")

	time.Sleep(50 * time.Millisecond)
	assertEqual(t, call(s.Get, "px"), "$-1\r\n")
	assertEqual(t, call(s.Get, "ex"), "$1\r\nv\r\n")
}

func TestSetAndGetErrors(t *testing.T) {
	s := newTestServer()
	call(s.Rpush, "list", "a")

	assertEqual(t, call(s.Set, "k"), "-ERR Set: Expected key value pair with/without optional Parameters: EX or PX\r\n")
	assertEqual(t, call(s.Set, "k", "v", "PX", "soon"), "-ERR Set: expiry must be an integer\r\n")
	assertEqual(t, call(s.Set, "k", "v", "XX", "5"), "-ERR Set: Unknown option, espected EX or PX\r\n")
	assertEqual(t, call(s.Get), "-ERR Get: Expected key parameter only\r\n")
	assertEqual(t, call(s.Get, "list"), "-ERR Cannot use Get on key with list value type\r\n")
}

func TestPushOrder(t *testing.T) {
	s := newTestServer()

	assertEqual(t, call(s.Rpush, "k", "a", "b"), ":2\r\n")
	assertEqual(t, call(s.Lpush, "k", "y", "z"), ":4\r\n") // each pushed to the front in turn

	assertEqual(t, call(s.Lrange, "k", "0", "-1"), "*4\r\n$1\r\nz\r\n$1\r\ny\r\n$1\r\na\r\n$1\r\nb\r\n")
	assertEqual(t, call(s.Llen, "k"), ":4\r\n")
}

func TestPushOntoStringFails(t *testing.T) {
	s := newTestServer()
	call(s.Set, "k", "v")

	assertEqual(t, call(s.Rpush, "k", "a"), "-ERR RPUSH: cannot append to a non list key-value pair\r\n")
	assertEqual(t, call(s.Lpush, "k", "a"), "-ERR LPUSH: cannot push to non list_key\r\n")
}

func TestLrange(t *testing.T) {
	s := newTestServer()
	call(s.Rpush, "k", "a", "b", "c", "d")

	tests := []struct {
		start, stop string
		want        string
	}{
		{"0", "1", "*2\r\n$1\r\na\r\n$1\r\nb\r\n"},
		{"1", "-2", "*2\r\n$1\r\nb\r\n$1\r\nc\r\n"},
		{"-2", "-1", "*2\r\n$1\r\nc\r\n$1\r\nd\r\n"},
		{"2", "100", "*2\r\n$1\r\nc\r\n$1\r\nd\r\n"},
		{"-100", "0", "*1\r\n$1\r\na\r\n"},
		{"3", "1", "*0\r\n"},
		{"10", "20", "*0\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.start+".."+tt.stop, func(t *testing.T) {
			assertEqual(t, call(s.Lrange, "k", tt.start, tt.stop), tt.want)
		})
	}
	assertEqual(t, call(s.Lrange, "missing", "0", "-1"), "*0\r\n")
}

func TestLlen(t *testing.T) {
	s := newTestServer()
	call(s.Set, "str", "v")

	assertEqual(t, call(s.Llen, "missing"), ":0\r\n")
	assertEqual(t, call(s.Llen, "str"), "-ERR LLEN: command was called on non key_list\r\n")
}

func TestLpop(t *testing.T) {
	s := newTestServer()
	call(s.Rpush, "k", "a", "b", "c")

	assertEqual(t, call(s.Lpop, "missing"), "$-1\r\n")
	assertEqual(t, call(s.Lpop, "k"), "$1\r\na\r\n")
	assertEqual(t, call(s.Lpop, "k", "2"), "*2\r\n$1\r\nb\r\n$1\r\nc\r\n")
	assertEqual(t, call(s.Lpop, "k", "-1"), "$-1\r\n") // list is gone by now
}

func TestIncr(t *testing.T) {
	s := newTestServer()
	call(s.Set, "neg", "-5")
	call(s.Set, "str", "abc")
	call(s.Rpush, "list", "a")

	assertEqual(t, call(s.Incr, "n"), ":1\r\n")
	assertEqual(t, call(s.Incr, "n"), ":2\r\n")
	assertEqual(t, call(s.Incr, "neg"), ":-4\r\n")
	assertEqual(t, call(s.Incr, "str"), "-ERR value is not an integer or out of range\r\n")
	assertEqual(t, call(s.Incr, "list"), "-ERR value is not an integer or out of range\r\n")
}

func TestInfoReportsRole(t *testing.T) {
	s := newTestServer()
	s.master_replid = "abc"

	assertEqual(t, call(s.Info), "$54\r\nrole:master\r\nmaster_replid:abc\r\nmaster_repl_offset:0\r\n\r\n")

	s.isMaster = false
	if got := call(s.Info); !strings.Contains(got, "role:slave") {
		t.Errorf("replica INFO = %q, want role:slave", got)
	}
}

func TestWaitWithNoReplicas(t *testing.T) {
	s := newTestServer()

	assertEqual(t, call(s.Wait, "0", "100"), ":0\r\n")
	assertEqual(t, call(s.Wait, "1", "20"), ":0\r\n") // times out with nobody to ack
	assertEqual(t, call(s.Wait, "x", "20"), "-ERR WAIT: first argument must be an integer\r\n")
}
