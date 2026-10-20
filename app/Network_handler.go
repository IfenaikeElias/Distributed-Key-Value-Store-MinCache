package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
)

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
			handleMulti(conn, args[1:], &inTransaction)
			continue
		}
		if cmd == "EXEC" {
			handleExec(s, conn, args[1:], &inTransaction, &transactionQueue)
			continue
		}

		if cmd == "DISCARD" {
			handleDiscard(conn, args[1:], &inTransaction, &transactionQueue)
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
			// handlers write their own error reply to the client; this is for server logs only
			fmt.Println("Command failed:", err)
			// failed writes must not reach replicas
			continue
		}
		// propagate plain command ran outside of transaction (no queueing) to replica
		if s.isMaster && isWriteCommand(cmd) {
			s.propagateToReplicas(append([]string{cmd}, args[1:]...))
		}
	}
}