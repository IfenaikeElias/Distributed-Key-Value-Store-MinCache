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

// replicaQueueSize is how many writes a replica may fall behind before it is dropped.
const replicaQueueSize = 10000

// addReplica registers conn as a replica and starts the goroutine that sends it writes.
func (s *Server) addReplica(conn net.Conn) {
	replica := &Replica{conn: conn, queue: make(chan []string, replicaQueueSize)}
	s.replicasMu.Lock()
	s.replicas = append(s.replicas, replica)
	s.replicasMu.Unlock()
	go replica.sendLoop()
}

// sendLoop writes queued commands to the replica, in order, until its queue is closed.
// It is the only goroutine writing to the replica's connection after the handshake.
func (r *Replica) sendLoop() {
	// batch: buffer commands while more are already queued, and hit the network once the
	// queue is empty. One write per command can't keep up with a pipelined master.
	w := bufio.NewWriterSize(r.conn, 64*1024)
	for args := range r.queue {
		w.Write(encodeArray(args))
		if len(r.queue) == 0 {
			w.Flush()
		}
	}
	w.Flush()
}

// propagateToReplicas queues a write for every replica; the network writes happen in each
// replica's sendLoop. Commands that change the store must call this while still holding
// s.DB.mutexLock, so replicas receive writes in exactly the order the master applied them.
func (s *Server) propagateToReplicas(args []string) {
	if !s.isMaster {
		return
	}
	s.replicasMu.Lock()
	defer s.replicasMu.Unlock()
	kept := s.replicas[:0]
	for _, replica := range s.replicas {
		select {
		case replica.queue <- args:
			kept = append(kept, replica)
		default:
			// never block here: we may be holding the store lock, which would stall every client.
			// a replica this far behind is dropped instead (Redis does the same)
			fmt.Println("Dropping replica: send queue full")
			close(replica.queue)
			replica.conn.Close()
		}
	}
	s.replicas = kept
	s.master_repl_offset += respSize(args)
}

// replicationOffset reads master_repl_offset under replicasMu, the lock
// propagateToReplicas holds while advancing it.
func (s *Server) replicationOffset() int {
	s.replicasMu.RLock()
	defer s.replicasMu.RUnlock()
	return s.master_repl_offset
}

// function to count how many replica have acknowledge the write they recieved 

// connectToMaster performs the replication handshake, then applies the master's
// writes until the connection drops.
//
// TODO: retry instead of giving up when the master is unreachable or the link drops:
//   - exponential backoff with a cap, plus random jitter so many replicas don't
//     reconnect in lockstep after a master restart (thundering herd / back pressure)
//   - only worth doing once FULLRESYNC sends a real snapshot: the master always
//     sends an empty RDB today, so a reconnecting replica would silently miss every
//     write made while it was disconnected
func (s *Server) connectToMaster() error {
	conn, err := net.Dial("tcp", s.replicaOf)
	if err != nil {
		return fmt.Errorf("Replica failed to connect to Master: %w", err)
	}
	defer conn.Close()
	// PING
	writeArray(conn, []string{"PING"})

	// Read PONG
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("handshake stage 1 (PING) failed: %w", err)
	}

	if response != "+PONG\r\n" {
        return fmt.Errorf("Expected '+PONG\\r\\n' got %q", response)
    }

	// Send REPLCONF
	writeArray(conn, []string{"REPLCONF", "listening-port", s.port})
	response, err = reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("handshake stage 2 (REPLCONF listening-port) failed: %w", err)
	}

	if response != "+OK\r\n" {
        return fmt.Errorf("Expected '+OK\\r\\n' got %q", response)
    }
    // write second REPLCONF (ready to sync data)
	writeArray(conn, []string{"REPLCONF", "capa", "psync2"})
	response, err = reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("handshake stage 3 (REPLCONF capa) failed: %w", err)
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