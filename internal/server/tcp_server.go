package server

import (
	"bufio"
	"fmt"
	"io"
	"kvsdb/internal/storage"
	"log"
	"net"
	"strings"
)

type Server struct {
	addr     string
	store    storage.Engine
	producer *storage.KafkaProducer
	listener net.Listener
}

func NewServer(addr string, store storage.Engine, producer *storage.KafkaProducer) *Server {
	return &Server{
		addr:     addr,
		store:    store,
		producer: producer,
	}
}

// Start boots up the TCP server and enters the accept loop
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.listener = l
	log.Printf("RESP server listening on %s (Redis-compatible)", s.addr)

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		// Handle each client concurrently
		go s.handleClient(conn)
	}
}

func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
	}
}

// handles the lifecycle of a single TCP client connection
func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	log.Printf("New client connected: %s", conn.RemoteAddr())

	for {
		//  Read and parse incoming RESP command
		cmd, err := ParseCommand(reader)
		if err != nil {
			if err == io.EOF {
				log.Printf("Client disconnected: %s", conn.RemoteAddr())
				return
			}
			log.Printf("Parser error from %s: %v", conn.RemoteAddr(), err)
			s.writeError(conn, fmt.Sprintf("ERR syntax error or invalid protocol: %v", err))
			return
		}

		// Route the command Case-Insensitive
		commandName := strings.ToUpper(cmd.Name)
		switch commandName {
		case "PING":
			s.handlePing(conn, cmd)
		case "GET":
			s.handleGet(conn, cmd)
		case "SET":
			s.handleSet(conn, cmd)
		case "DEL":
			s.handleDel(conn, cmd)
		default:
			s.writeError(conn, fmt.Sprintf("ERR unknown command '%s'", cmd.Name))
		}
	}
}

// Command Handlers
func (s *Server) handlePing(conn net.Conn, cmd *Command) {
	// PING can have optional message, but usually just returns simple string +PONG\r\n
	if len(cmd.Args) > 0 {
		s.writeBulkString(conn, cmd.Args[0])
	} else {
		conn.Write([]byte("+PONG\r\n"))
	}
}

func (s *Server) handleGet(conn net.Conn, cmd *Command) {
	if len(cmd.Args) != 1 {
		s.writeError(conn, "ERR wrong number of arguments for 'get' command")
		return
	}
	key := cmd.Args[0]
	val := s.store.Get(key)

	// In RocksDB, missing keys return ""
	if val == "" {
		conn.Write([]byte("$-1\r\n")) // RESP Null Bulk String
	} else {
		s.writeBulkString(conn, val)
	}
}

func (s *Server) handleSet(conn net.Conn, cmd *Command) {
	if len(cmd.Args) != 2 {
		s.writeError(conn, "ERR wrong number of arguments for 'set' command")
		return
	}
	key := cmd.Args[0]
	val := cmd.Args[1]

	// CQRS Rule: SET writes strictly to the Kafka WAL first
	err := s.producer.Set("kvsdb-wal", key, val)
	if err != nil {
		s.writeError(conn, fmt.Sprintf("ERR failed to commit to WAL: %v", err))
		return
	}

	// Simple String OK
	conn.Write([]byte("+OK\r\n"))
}

func (s *Server) handleDel(conn net.Conn, cmd *Command) {
	if len(cmd.Args) != 1 {
		s.writeError(conn, "ERR wrong number of arguments for 'del' command")
		return
	}
	key := cmd.Args[0]

	// Tombstone record
	err := s.producer.Set("kvsdb-wal", key, "")
	if err != nil {
		s.writeError(conn, fmt.Sprintf("ERR failed to commit deletion to WAL: %v", err))
		return
	}

	// Return 1 indicating 1 key was deleted
	conn.Write([]byte(":1\r\n"))
}

// RESP Serialization Helpers

func (s *Server) writeBulkString(conn net.Conn, val string) {
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
	conn.Write([]byte(resp))
}

func (s *Server) writeError(conn net.Conn, msg string) {
	resp := fmt.Sprintf("-%s\r\n", msg)
	conn.Write([]byte(resp))
}
