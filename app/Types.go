package main

import (
	"net"
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