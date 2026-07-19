package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const CRLF = "\r\n"


// Command handler type
type CommandHandler func(conn net.Conn, args ...string) error

type value struct {
	strVal      string
	listVal     []string
	isListValue bool
	expiryTime  time.Time
}

// struct for KV store
type Store struct {
	mutexLock sync.RWMutex
	KVStore   map[string]value
}

type function struct {
	name        string
	functionHandlerName CommandHandler
	arguements   []string
}

// struct replica
type Replica struct {
	conn net.Conn
	offset int
}
// struct for server
type Server struct {
	Listener         	net.Listener
	port             	string
	DB               	*Store
	isMaster 		 	bool
	replicaOf 			string // host port of master if the server is a replica
	replicas 			[]Replica // connected replica server can use only if is a master
	replicasMu 			sync.RWMutex
	replOffset 			int
	master_repl_offset  int
	master_replid 	 	string
}

// intialize store
var store = &Store{
	KVStore: make(map[string]value),
}



// ------ utils for establishing connections ------

func (s *Server) ListenForConn() {
	/*
		func to listen for connections and handle error
	*/
	port := "0.0.0.0:" + s.port
	ln, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("Error starting server:", err.Error())
		os.Exit(1)
	}
	s.Listener = ln
}

func (s *Server) AcceptConn() (net.Conn, error) {
	/*
		func to accept connections and handle error
	*/
	conn, err := s.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return conn, nil
}

//  ---- utils for writing RESP responses -----

func writeStr(conn net.Conn, s string) {
	resp := "+" + s + CRLF
	conn.Write([]byte(resp))
}

func writeErr(conn net.Conn, msg string) {
	conn.Write([]byte("-ERR " + msg + CRLF))
}

func writeBulkStr(conn net.Conn, isNull bool, s string) {
	if isNull {
		conn.Write([]byte("$-1" + CRLF))
	} else {
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
		conn.Write([]byte(resp))
	}
}

func writeInt(conn net.Conn, n int) {
	conn.Write([]byte(fmt.Sprintf(":%d\r\n", n)))
}

func writeArray(conn net.Conn, items []string) {
	var resp strings.Builder
	fmt.Fprintf(&resp, "*%d\r\n", len(items))
	for _, items := range items {
		fmt.Fprintf(&resp, "$%d\r\n%s\r\n", len(items), items)
	}
	conn.Write([]byte(resp.String()))
}

func writeNullArray(conn net.Conn) {
	conn.Write([]byte("*-1\r\n"))
}

func writeRDBFile(conn net.Conn, content string) {
    fileContent := fmt.Sprintf("$%d\r\n%s", len(content), content)
    conn.Write([]byte(fileContent))
}

func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "RPUSH", "LPUSH", "LPOP", "INCR":
		return true
	}
	return false
}



// ----- Standard RESP Parser ---------

func parseRESP(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("parseRESP: %w", err)
	}
	line = strings.TrimRight(line, "\r\n")

	// must start with * (array)
	if line == "" || line[0] != '*' {
		return nil, fmt.Errorf("ERROR: expected array")
	}

	var count int
	if _, err := fmt.Sscanf(line[1:], "%d", &count); err !=  nil {
		return nil, fmt.Errorf("bad array count %w", err)
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		// read $<len>
		lenLine, err := reader.ReadString('\n')
		if err != nil {
			return nil, fmt.Errorf("parseRESP reading length: %w", err)
		}
		// check if the resp array is bad, i.e has alphabet in place of length
		lenLine = strings.TrimRight(lenLine, "\r\n")
		if lenLine == "" || lenLine[0] != '$' {
			return nil, fmt.Errorf("parseRESP: expected bulk string, got %q", lenLine)
		}
		// how many byte of string to read as stated by lenLine e.g $3
		n, err := strconv.Atoi(lenLine[1:])
		if err !=  nil {
			return nil, fmt.Errorf("bad bulk lenght: %w", err)
		}
		// read the actual value
		buff := make([]byte, n + 2) // + 2 to accomodate the extra "\r\n"
		if _, err :=  io.ReadFull(reader, buff); err != nil {
			return nil, fmt.Errorf("parseRESP reading value: %w", err)
		}
		args = append(args, string(buff[:n]))
	}
	return args, nil
}

// ------- function to handle connection ------
func handleConnection(conn net.Conn, Commands map[string]CommandHandler, s *Server) {
	
	defer conn.Close()
	reader := bufio.NewReader(conn)
	inTransaction := false
	transactionQueue := []function{}
	for {
		args, err := parseRESP(reader)
		if err != nil {
			writeErr(conn, err.Error())
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		if !s.isMaster && isWriteCommand(cmd) {
			writeErr(conn, "Replicas are not allowed to write")
			continue
		}


		if cmd == "REPLCONF" {
			if len(args) > 2 && strings.ToUpper(args[1]) == "ACK" {
				if offSet, err := strconv.Atoi(args[2]); err == nil {
					s.replicasMu.Lock()
					for i := range s.replicas {
						if s.replicas[i].conn == conn {
							s.replicas[i].offset = offSet
							break
						}
					}
					s.replicasMu.Unlock()
				}
				continue
			}
				writeStr(conn, "OK")
				continue
			}
		
		// // respond to "REPLCONF from handshake"
		// if cmd == "REPLCONF" {
		// 	writeStr(conn, "OK")
		// 	continue
		// }

		// Respond to "PSYNC from handshake"
		if cmd == "PSYNC" {
			writeStr(conn, fmt.Sprintf("FULLRESYNC %s %d", s.master_replid, s.master_repl_offset))
			emptyRDB, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
			writeRDBFile(conn, string(emptyRDB))

			// register this conn as replica
			s.replicasMu.Lock()
			s.replicas = append(s.replicas, Replica{conn:conn, offset: 0})
			s.replicasMu.Unlock()
			continue
		}
		
		if cmd == "INFO" {
			s.Info(conn, args[1:]...)
			continue
		}
		if cmd == "MULTI" {
			if len(args[1:]) != 0 {
				writeErr(conn, "MULTI: command takes no arguement")
				continue
			}
			if inTransaction {
				writeErr(conn, "MULTI cannot be called against MULTI")
				continue
			}
			inTransaction = true
			writeStr(conn, "OK")
			continue
		}
		if cmd == "EXEC" {
			if len(args[1:]) != 0 {
				writeErr(conn, "EXEC: command takes no arguement")
				continue
			}
			if !inTransaction {
				writeErr(conn, "EXEC without MULTI")
				continue
			}
			inTransaction = false
			// Write Array head
			if len(transactionQueue) == 0 {
				writeArray(conn, []string{})
				continue
			}
			// write transaction to replica
			conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(transactionQueue))))
			for _, command := range transactionQueue {
				if err := command.functionHandlerName(conn, command.arguements...); err != nil {
					writeErr(conn, err.Error())
					continue
				}
				if s.isMaster && isWriteCommand(command.name) {
					s.propagateToReplicas(append([]string{command.name}, command.arguements...))
				}
			}
			transactionQueue = []function{}
			continue
		}

		if cmd == "DISCARD" {
			if len(args[1:]) != 0 {
				writeErr(conn, "DISCARD: commad takes no arguement")
				continue
			}
			if !inTransaction {
				writeErr(conn, "DISCARD without MULTI")
				continue
			}
			inTransaction = false
			transactionQueue = []function{}
			writeStr(conn, "OK")
			continue
		}
        
		handler, ok := Commands[cmd]
		if !ok {
			writeErr(conn, "unknown command")
			continue
		}
		if inTransaction {
			transactionQueue = append(
				transactionQueue, 
				function{
					name: cmd, 
					functionHandlerName: handler, 
					arguements: args[1:]})
			writeStr(conn, "QUEUED")
			continue
		} else if err := handler(conn, args[1:]...); err != nil {
			writeErr(conn, err.Error())
		}
		// propagate plain command ran outside of transaction (no queueing) to replica
		if s.isMaster && isWriteCommand(cmd) {
			s.propagateToReplicas(append([]string{cmd}, args[1:]...))
		}
	}
}

// function ton propagate commands to replicas
func (s *Server) propagateToReplicas(args []string) {
	s.replicasMu.Lock()
	for _, replica := range s.replicas {
		writeArray(replica.conn, args)
	}
	// after propagation of command from master to replica is complete
	// update the replicaOffset in master i.e maste_repl_offset
	s.master_repl_offset += respSize(args)
	s.replicasMu.Unlock()
}

// function to count how many replica have acknowledge the write they recieved 

func (s *Server) connectToMaster() error {
	conn, err := net.Dial("tcp", s.replicaOf)
	if err != nil {
		return fmt.Errorf("Replica failed to connect to Master: %w", err)
	}
	// PING
	writeArray(conn, []string{"PING"})

	// Read PONG
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("stage 1 of Handshake failed")
	}

	if response != "+PONG\r\n" {
        return fmt.Errorf("Expected '+PONG\\r\\n' got %q", response)
    }

	// Send REPLCONF
	writeArray(conn, []string{"REPLCONF", "listening-port", s.port})
	response, err = reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("stage 2 of Handshake failed")
	}

	if response != "+OK\r\n" {
        return fmt.Errorf("Expected '+OK\\r\\n' got %q", response)
    }
    // write second REPLCONF (ready to sync data)
	writeArray(conn, []string{"REPLCONF", "capa", "psync2"})
	response, err = reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("stage 2 of Handshake failed")
	}

	if response != "+OK\r\n" {
        return fmt.Errorf("Expected '+OK\\r\\n' got %q", response)
    }

	// Send PSYNC
	writeArray(conn, []string{"PSYNC", "?", "-1"})
	_, err = reader.ReadString('\n')
	if err !=  nil {
		return fmt.Errorf("RDB header failed: %w", err)
	}
	rdbHeader, err := reader.ReadString('\n')
	if err !=  nil {
		return fmt.Errorf("Failed to read from RDB file: %w", err)
	}
	rdbHeader = strings.TrimRight(rdbHeader, "\r\n")
	var rdbLen int
	fmt.Sscanf(rdbHeader[1:], "%d", &rdbLen)
	rdbBuffer := make([]byte, rdbLen)
	io.ReadFull(reader, rdbBuffer)
	s.listenToMaster(reader, conn)
    return nil
}
	

func respSize(args []string) int {
    size := len(fmt.Sprintf("*%d\r\n", len(args)))
    for _, arg := range args {
        size += len(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
    }
    return size
}

func (s *Server) listenToMaster(reader *bufio.Reader, conn net.Conn) {
    for {
        args, err := parseRESP(reader)
        if err != nil {
            fmt.Println("Lost connection to master:", err)
            return
        }
        if len(args) == 0 {
            continue
        }
        cmd := strings.ToUpper(args[0])
        switch cmd {
        case "SET":
            s.Set(nil, args[1:]...)
        case "RPUSH":
            s.Rpush(nil, args[1:]...)
        case "LPUSH":
            s.Lpush(nil, args[1:]...)
        case "INCR":
            s.Incr(nil, args[1:]...)
        case "REPLCONF":
            if len(args) > 1 && strings.ToUpper(args[1]) == "GETACK" {
				s.replOffset += respSize((args))
                writeArray(conn, []string{"REPLCONF", "ACK", strconv.Itoa(s.replOffset)})
            }
        }
        // increment offset by size of received message
        s.replOffset += respSize(args)
    }
}

// Utils for generate master_replid
func GenerateReplicationID() string {
	bytes := make([]byte, 20) // 20 bytes = 40 hex characters
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
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

	if len(args) == 1 {
		popped := entity.listVal[0]
		entity.listVal = entity.listVal[1:]
		s.DB.KVStore[key] = entity
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
	s.DB.KVStore[key] = entity
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
				s.DB.KVStore[key] = entity
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
			s.DB.KVStore[key] = entity
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

func (s *Server) Wait(conn net.Conn, args...string) error {
	if len(args) != 2 {
		writeErr(conn, "WAIT: command takes two arguements")
	}

	numReplicasWanted, err := strconv.Atoi(args[0])
	if err !=  nil {
		writeErr(conn, "WAIT: first argument must be an integer")
		return nil
	}

	timeOut, err := strconv.Atoi(args[1])
	if err != nil {
		writeErr(conn, "WAIT: second argument must be an integer")
		return nil
	}

	targetOffSet := s.master_repl_offset

	// ask replicas to report their current state
	s.propagateToReplicas([]string{"REPLCONF", "GETACK", "*"})

	// convert waiting time to millisecond and calculate end of waiting
	deadline := time.Now().Add(time.Duration(timeOut) * time.Millisecond)
	for time.Now().Before(deadline) {
		// count all eligible replicas for wait command
		count := 0
		for _, r := range s.replicas { 
			if r.offset >= targetOffSet {
				count ++
			}
		}
		if count >= numReplicasWanted {
			writeInt(conn, count)
		}
	} 
	return nil
}

// helpers for WAIT command
/*
We the master has to see how many replicas has acknowledge the pre
previous writes. The only way to check this is if the master_repl_offset == replica_offset (for the replicas)
so we would have to iterate over all replicas and return get/count the replicas 
that satify this condition.

so we propagate REPLCONF GETACK to all replicas, they reponsod with ACK <offset>
and we can see which replica has caught up to the recent write (replica_offset == master_repl_offset).
we count them.
*/

// func (s *Server) countA


// func (s *Server) Wait(conn net.Conn, args...string) error {
// 	if len(args) !=  2 {
// 		writeErr(conn, "WAIT: commnand takes two arguemnet")
// 	}
// 	noOfReplicaToWaitFor, err := strconv.ParseInt(args[0], 10, 64)
// 	if err != nil {
// 		writeErr(conn, "WAIT: command expected no of replicas to wait for as an integer value")
// 		return nil
// 	}
// 	timeToWait, err := strconv.ParseInt(args[1], 10, 64) 
// 	if err !=  nil {
// 		writeErr(conn, "time out has to be an integer value")
// 		return nil
// 	}
//     timeOut := time.Now().Add(time.Duration(time.Duration(timeToWait) * time.Second))

// 	for time.Now().After(timeOut) {
// 		n := int(noOfReplicaToWaitFor)
// 		if len(s.replicas) <=  n {
// 			writeInt(conn, n)
// 			return nil
// 		}
// 	}


	

// ---- main ------
func main() {
	// command name to methods
	portFlag := flag.String("port", "6379", "port for server to listen on.")
	replicaFlag := flag.String("replicaof", "", "replica flag")

	flag.Parse()
	s := &Server{
		port: *portFlag,
		DB:   store,
		
	}

	Commands := map[string]CommandHandler{
		"PING":   Ping,
		"ECHO":   Echo,
		"SET":    s.Set,
		"GET":    s.Get,
		"RPUSH":  s.Rpush,
		"LRANGE": s.Lrange,
		"LPUSH":  s.Lpush,
		"LLEN":   s.Llen,
		"LPOP":   s.Lpop,
		"BLPOP":  s.Blpop,
		"INCR":   s.Incr, 
		"WAIT" : s.Wait,
	}

	if *replicaFlag != "" {
    s.isMaster = false
    // Replace space with colon because net.Dial expects "host:port"
	s.replicaOf = strings.Replace(*replicaFlag, " ", ":", 1)
	s.master_repl_offset = 0
	s.isMaster = false
	s.master_replid = GenerateReplicationID()
	} else {
		s.isMaster = true
		s.master_repl_offset = 0
		s.master_replid = GenerateReplicationID()
	}
	if !s.isMaster {
		// connect to master and start syncing
		go s.connectToMaster()
	}
	s.ListenForConn()

	for {
		conn, err := s.AcceptConn()
		if err != nil {
			fmt.Println("Failed to a ccept connection: ", err.Error())
			continue
		}
		go handleConnection(conn, Commands, s)
	}
}
