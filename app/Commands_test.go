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
