package main

import (
	"flag"
	"fmt"
	"strings"
)

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