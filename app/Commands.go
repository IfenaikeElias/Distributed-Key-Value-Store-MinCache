package main

import (
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "RPUSH", "LPUSH", "LPOP", "INCR":
		return true
	}
	return false
}

// saveList writes a list back to the store, deleting the key once the list is empty.
// Caller must hold s.mutexLock.
func (s *Store) saveList(key string, entity value) {
	if len(entity.listVal) == 0 {
		delete(s.KVStore, key)
		return
	}
	s.KVStore[key] = entity
}

// -------- Functions --------

// PING function
func Ping(conn net.Conn, args ...string) error {
	if len(args) > 0 {
		return fmt.Errorf("Ping takes no arguement")
	}
	writeStr(conn, "PONG")
	return nil
}

// ECHO function
func Echo(conn net.Conn, args ...string) error {
	message := strings.Join(args, " ")
	writeBulkStr(conn, false, message)
	return nil
}

// Set Method
func (s *Server) Set(conn net.Conn, args ...string) error {
	if noArgs := len(args); noArgs < 2 {
		writeErr(conn, "Set: Expected key value pair with/without optional Parameters: EX or PX")
		return fmt.Errorf("Set: expected key valuce pair, got %d arguements", len(args))
	} else if noArgs > 4 {
		writeErr(conn, "Set: Too much parameters, This command only supports key value pair and PX or EX options")
		return fmt.Errorf("Set: expected key valuce pair with/without parameters PX and EX, got %d arguements", len(args))
	}
	key, val := args[0], args[1]
	entry := value{strVal: val}

	if len(args) == 4 {
		expiryOption := strings.ToUpper(args[2])
		expTime, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil {
			writeErr(conn, "Set: expiry must be an integer")
			return fmt.Errorf("Set: expiry must be an integer")
		}
		switch expiryOption {
		case "EX":
			entry.expiryTime = time.Now().Add(time.Duration(expTime) * time.Second)
		case "PX":
			entry.expiryTime = time.Now().Add(time.Duration(expTime) * time.Millisecond)
		default:
			writeErr(conn, "Set: Unknown option, espected EX or PX")
			return fmt.Errorf("Set: Command does not support Option %s arguements", args[2])
		}
	}

	s.DB.mutexLock.Lock()
	s.DB.KVStore[key] = entry
	s.DB.mutexLock.Unlock()
	if s.isMaster {
		writeStr(conn, "OK")
	}
	return nil
}

// Get Method

func (s *Server) Get(conn net.Conn, args ...string) error {
	if noArgs := len(args); noArgs != 1 {
		writeErr(conn, "Get: Expected key parameter only")
		return fmt.Errorf("Get: expected a key arguements got %d arguements", len(args))
	}
	key := args[0]
	s.DB.mutexLock.RLock()
	entry, ok := s.DB.KVStore[key]
	s.DB.mutexLock.RUnlock()
	if !ok {
		writeBulkStr(conn, true, "")
		return nil
	}
	if !entry.expiryTime.IsZero() && time.Now().After(entry.expiryTime) {
		writeBulkStr(conn, true, "")
		return nil
	}
	if entry.isListValue {
		writeErr(conn, "Cannot use Get on key with list value type")
		return nil
	}
	writeBulkStr(conn, false, entry.strVal)
	return nil
}

// RPUSH function
func (s *Server) Rpush(conn net.Conn, args ...string) error {
	if len(args) < 2 {
		writeErr(conn, "RPUSH: Command expects at leat 3 arguements RPUSH list_key foo")
		return fmt.Errorf("RPUSH: Command expects at leat 3 arguements: RPUSH list_key foo but got %d argeuemnts", len(args))
	}
	key := args[0]
	elements := args[1:]

	s.DB.mutexLock.Lock()
	entry, ok := s.DB.KVStore[key]
	if !ok {
		entry = value{isListValue: true,
			listVal: []string{},
		}
	} else if !entry.isListValue {
		s.DB.mutexLock.Unlock()
		writeErr(conn, "RPUSH: cannot append to a non list key-value pair")
		return fmt.Errorf("RPUSH: cannot append to a non list key-value pair")
	}
	entry.listVal = append(entry.listVal, elements...)
	s.DB.KVStore[key] = entry
	s.DB.mutexLock.Unlock()
	
	writeInt(conn, len(entry.listVal))
	return nil
}

// LRANGE function
func (s *Server) Lrange(conn net.Conn, args ...string) error {
	if len(args) != 3 {
		writeErr(conn, "LRANGE: command expects 3 arguements: List_key start_index stop_index + 1")
		return fmt.Errorf("LRANGE expected 3 arguements but %d was provided", len(args))
	}
	key := args[0]
	start, err := strconv.Atoi(args[1])
	if err != nil {
		writeErr(conn, "LRANGE: command expected an integer index")
		return fmt.Errorf("LRANGE expected an integer index")
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		writeErr(conn, "LRANGE: command expected an integer index")
		return fmt.Errorf("LRANGE expected an integer index")
	}
	s.DB.mutexLock.Lock()
	entity, ok := s.DB.KVStore[key]
	s.DB.mutexLock.Unlock()
	if !ok {
		writeArray(conn, []string{})
		return nil
	}
	if !entity.isListValue {
		writeErr(conn, "LRANGE: command expects key with value of type: list")
		return fmt.Errorf("LRANGE expects key with value of type: list")
	}
	if start < 0 {
		if -start > len(entity.listVal) {
			start = 0
		} else {
			start += len(entity.listVal)
		}
	}
	if stop < 0 {
		if -stop > len(entity.listVal) {
			stop = 0
		} else {
			stop += len(entity.listVal)
		}
	}
	if start >= len(entity.listVal) || start > stop {
		writeArray(conn, []string{})
		return nil
	} else if stop >= len(entity.listVal) {
		writeArray(conn, entity.listVal[start:])
		return nil
	}
	writeArray(conn, entity.listVal[start:stop+1])
	return nil
}

// Lpush function
func (s *Server) Lpush(conn net.Conn, args ...string) error {
	if len(args) < 2 {
		writeErr(conn, "LPUSH: Expected at least 2 arguements: list_key elemtents_to_push")
		return fmt.Errorf("LPUSH: LPUSH recieved less than 2 arguements but expected: list_key, elements_to_push")
	}
	key := args[0]
	elements := args[1:]
	slices.Reverse(elements)
	s.DB.mutexLock.Lock()
	entity, ok := s.DB.KVStore[key]
	if !ok {
		entity = value{isListValue: true,
			listVal: []string{},
		}
	} else if !entity.isListValue {
		s.DB.mutexLock.Unlock()
		writeErr(conn, "LPUSH: cannot push to non list_key")
		return fmt.Errorf("LPUSH: attempted to push to non list_key")
	}
	entity.listVal = append(elements, entity.listVal...)
	s.DB.KVStore[key] = entity
	s.DB.mutexLock.Unlock()
	
	writeInt(conn, len(entity.listVal))
	return nil
}

// Llen function
func (s *Server) Llen(conn net.Conn, args ...string) error {
	if len(args) != 1 {
		writeErr(conn, "LLEN: command expects 1 arguements: list_key")
		return fmt.Errorf("LLEN did not get 1 aguement: list_key")
	}
	key := args[0]
	s.DB.mutexLock.RLock()
	entity, ok := s.DB.KVStore[key]
	s.DB.mutexLock.RUnlock()
	if !ok {
		writeInt(conn, 0)
		return nil
	} else if !entity.isListValue {
		writeErr(conn, "LLEN: command was called on non key_list")
		return fmt.Errorf("LLEN: key is non key_list")
	}
	writeInt(conn, len(entity.listVal))
	return nil
}

// Lpop function
func (s *Server) Lpop(conn net.Conn, args ...string) error {
	if len(args) < 1 || len(args) > 2 {
		writeErr(conn, "LPOP: command expected 1 argument")
		return fmt.Errorf("LPOP: commnand EXpected 1 arguement but got %d", len(args))
	}
	key := args[0]
	s.DB.mutexLock.Lock()
	entity, ok := s.DB.KVStore[key]
	if !ok {
		s.DB.mutexLock.Unlock()
		writeBulkStr(conn, true, "")
		return nil
	} else if !entity.isListValue {
		s.DB.mutexLock.Unlock()
		writeErr(conn, "LPOP: command got a non key_lest arguement")
		return fmt.Errorf("LPOP: command got a non key_lest arguement %s", key)
	}

	if len(entity.listVal) == 0 {
		s.DB.mutexLock.Unlock()
		writeBulkStr(conn, true, "")
		return nil
	}

	if len(args) == 1 {
		popped := entity.listVal[0]
		entity.listVal = entity.listVal[1:]
		s.DB.saveList(key, entity)
		s.DB.mutexLock.Unlock()
		writeBulkStr(conn, false, popped)
		return nil
	}

	count, err := strconv.Atoi(args[1])
	if err != nil || count < 0 {
		s.DB.mutexLock.Unlock()
		writeErr(conn, "LPOP: count must be a positive integer")
		return nil
	}
	if count > len(entity.listVal) {
		count = len(entity.listVal)
	}
	popped := entity.listVal[:count]
	entity.listVal = entity.listVal[count:]
	s.DB.saveList(key, entity)
	s.DB.mutexLock.Unlock()
	if len(popped) == 1 {
		writeBulkStr(conn, false, popped[0])
		return nil
	}
	writeArray(conn, popped)
	return nil
}

func (s *Server) Blpop(conn net.Conn, args ...string) error {
	if len(args) != 2 {
		if conn != nil {
			writeErr(conn, "BLPOP: command expects two arguements: list_key timeout")
		}
		return nil
	}
	key := args[0]
	timeoutValue, err := strconv.ParseFloat(args[1], 64)
	if err != nil || timeoutValue < 0 {
		if conn != nil {
			writeErr(conn, "BLPOP: timeout must be a positive number")
		}
		return nil
	}

	var deadline time.Time
	indefinite := timeoutValue == 0
	if !indefinite {
		deadline = time.Now().Add(time.Duration(timeoutValue * float64(time.Second)))
	}

	if !indefinite {
		for time.Now().Before(deadline) {
			s.DB.mutexLock.Lock()
			entity, ok := s.DB.KVStore[key]
			if !ok {
				s.DB.mutexLock.Unlock()
				continue
			} else if !entity.isListValue {
				s.DB.mutexLock.Unlock()
				if conn != nil {
					writeErr(conn, "BLPOP: command got a non list_key arguement")
				}
				return nil
			}
			if len(entity.listVal) > 0 {
				popped := entity.listVal[0]
				entity.listVal = entity.listVal[1:]
				s.DB.saveList(key, entity)
				s.DB.mutexLock.Unlock()
				if conn != nil {
					writeArray(conn, []string{key, popped})
				}
				if s.isMaster {
					s.propagateToReplicas([]string{"LPOP", key})
				}
				return nil
			}
			s.DB.mutexLock.Unlock()
		}
		if conn != nil {
			writeNullArray(conn)
		}
		return nil
	}

	// indefinite
	for {
		s.DB.mutexLock.Lock()
		entity, ok := s.DB.KVStore[key]
		if !ok {
			s.DB.mutexLock.Unlock()
			continue
		} else if !entity.isListValue {
			s.DB.mutexLock.Unlock()
			if conn != nil {
				writeErr(conn, "BLPOP: command got a non list_key arguement")
			}
			return nil
		}
		if len(entity.listVal) > 0 {
			popped := entity.listVal[0]
			entity.listVal = entity.listVal[1:]
			s.DB.saveList(key, entity)
			s.DB.mutexLock.Unlock()
			if conn != nil {
				writeArray(conn, []string{key, popped})
			}
			if s.isMaster {
				s.propagateToReplicas([]string{"LPOP", key})
			}
			return nil
		}
		s.DB.mutexLock.Unlock()
	}
}

// Incr function
func (s *Server) Incr(conn net.Conn, args ...string) error {
	if len(args) != 1 {
		writeErr(conn, "INCR: command expects one arguement")
		return nil
	}
	key := args[0]
	s.DB.mutexLock.Lock()
	entity, ok := s.DB.KVStore[key]
	s.DB.mutexLock.Unlock()
	if !ok {
		s.DB.mutexLock.Lock()
		s.DB.KVStore[key] = value{strVal: "1"}
		s.DB.mutexLock.Unlock()
		writeInt(conn, 1)
		return nil
	}
	if !entity.isListValue {
		number, err := strconv.Atoi(entity.strVal)
		if err != nil {
			writeErr(conn, "value is not an integer or out of range")
			return nil
		}
		strNumber := strconv.Itoa(number + 1)
		updatedNumber, _ := strconv.Atoi(strNumber)
		entity.strVal = strNumber
		s.DB.mutexLock.Lock()
		s.DB.KVStore[key] = entity
		s.DB.mutexLock.Unlock()
		writeInt(conn, updatedNumber)
		return nil
	}
	writeErr(conn, "value is not an integer or out of range")
	return nil
}


func (s *Server) Info(conn net.Conn, args ...string) error {
	if len(args) > 1 {
		writeErr(conn, "INFO: command takes at most one argument")
		return nil
	}
	var info strings.Builder
	if s.isMaster {
		info.WriteString("role:master\r\n")
	} else {
		info.WriteString("role:slave\r\n")
	}
	info.WriteString(fmt.Sprintf("master_replid:%s\r\n", s.master_replid))
	info.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", s.master_repl_offset))
	writeBulkStr(conn, false, info.String())
	return nil
}

func (s *Server) Wait(conn net.Conn, args ...string) error {
	if len(args) != 2 {
		writeErr(conn, "WAIT: command takes two arguments")
		return nil
	}

	numReplicasWanted, err := strconv.Atoi(args[0])
	if err != nil {
		writeErr(conn, "WAIT: first argument must be an integer")
		return nil
	}

	timeOut, err := strconv.Atoi(args[1])
	if err != nil {
		writeErr(conn, "WAIT: second argument must be an integer")
		return nil
	}

	targetOffSet := s.master_repl_offset

	// check if already caught up before asking
	s.replicasMu.RLock()
	count := 0
	for _, r := range s.replicas {
		if r.offset >= targetOffSet {
			count++
		}
	}
	s.replicasMu.RUnlock()
	if count >= numReplicasWanted {
		writeInt(conn, count)
		return nil
	}

	// ask replicas to report their current state
	s.propagateToReplicas([]string{"REPLCONF", "GETACK", "*"})

	deadline := time.Now().Add(time.Duration(timeOut) * time.Millisecond)
	for time.Now().Before(deadline) {
		s.replicasMu.RLock()
		count := 0
		for _, r := range s.replicas {
			if r.offset >= targetOffSet {
				count++
			}
		}
		s.replicasMu.RUnlock()

		if count >= numReplicasWanted {
			writeInt(conn, count)
			return nil
		}
	}

	// timed out — report however many caught up in time
	s.replicasMu.RLock()
	count = 0
	for _, r := range s.replicas {
		if r.offset >= targetOffSet {
			count++
		}
	}
	s.replicasMu.RUnlock()
	writeInt(conn, count)
	return nil
}

/*
We the master has to see how many replicas has acknowledge the pre
previous writes. The only way to check this is if the master_repl_offset == replica_offset (for the replicas)
so we would have to iterate over all replicas and return get/count the replicas 
that satify this condition.

so we propagate REPLCONF GETACK to all replicas, they reponsod with ACK <offset>
and we can see which replica has caught up to the recent write (replica_offset == master_repl_offset).
we count them.
*/