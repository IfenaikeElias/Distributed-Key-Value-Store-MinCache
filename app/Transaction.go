package main

import (
	"fmt"
	"net"
)

// handleMulti implements the MULTI command.
// args is the command's arguments (i.e. everything after "MULTI").
func handleMulti(conn net.Conn, args []string, inTransaction *bool) {
	if len(args) != 0 {
		writeErr(conn, "MULTI: command takes no arguement")
		return
	}
	if *inTransaction {
		writeErr(conn, "MULTI cannot be called against MULTI")
		return
	}
	*inTransaction = true
	writeStr(conn, "OK")
}

// handleExec implements the EXEC command.
// args is the command's arguments (i.e. everything after "EXEC").
func handleExec(s *Server, conn net.Conn, args []string, inTransaction *bool, transactionQueue *[]function) {
	if len(args) != 0 {
		writeErr(conn, "EXEC: command takes no arguement")
		return
	}
	if !*inTransaction {
		writeErr(conn, "EXEC without MULTI")
		return
	}
	*inTransaction = false
	// Write Array head
	if len(*transactionQueue) == 0 {
		writeArray(conn, []string{})
		return
	}
	// write transaction to replica
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(*transactionQueue))))
	for _, command := range *transactionQueue {
		if err := command.functionHandlerName(conn, command.arguements...); err != nil {
			// handler already wrote the error reply into the EXEC array
			fmt.Println("Command failed in EXEC:", err)
			continue
		}
		if command.name == "RPUSH" || command.name == "LPUSH" {
			s.serveBlockedClients(command.arguements[0])
		}
	}
	*transactionQueue = []function{}
}

// handleDiscard implements the DISCARD command.
// args is the command's arguments (i.e. everything after "DISCARD").
func handleDiscard(conn net.Conn, args []string, inTransaction *bool, transactionQueue *[]function) {
	if len(args) != 0 {
		writeErr(conn, "DISCARD: commad takes no arguement")
		return
	}
	if !*inTransaction {
		writeErr(conn, "DISCARD without MULTI")
		return
	}
	*inTransaction = false
	*transactionQueue = []function{}
	writeStr(conn, "OK")
}