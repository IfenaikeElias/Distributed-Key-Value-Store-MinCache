package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

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

// replicationOffset reads master_repl_offset under replicasMu, the lock
// propagateToReplicas holds while advancing it.
func (s *Server) replicationOffset() int {
	s.replicasMu.RLock()
	defer s.replicasMu.RUnlock()
	return s.master_repl_offset
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
        case "LPOP":
            s.Lpop(nil, args[1:]...)
        case "REPLCONF":
            if len(args) > 1 && strings.ToUpper(args[1]) == "GETACK" {
				writeArray(conn, []string{"REPLCONF", "ACK", strconv.Itoa(s.replOffset)})
				s.replOffset += respSize((args))
                continue
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