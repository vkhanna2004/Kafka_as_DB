package server

import (
	"bufio"
	"fmt"
	"io"
	"kvsdb/internal/storage"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Server struct {
	addr        string
	store       storage.Engine
	producer    *storage.KafkaProducer
	listener    net.Listener
	walTopic    string
	snapshotDir string
	cacheMutex  sync.RWMutex
	rywCache    map[string]cacheEntry
}

type cacheEntry struct {
	value     string
	partition int32
	offset    int64
	expireAt  int64
}

func NewServer(addr string, store storage.Engine, producer *storage.KafkaProducer, walTopic string, snapshotDir string) *Server {
	return &Server{
		addr:        addr,
		store:       store,
		producer:    producer,
		walTopic:    walTopic,
		snapshotDir: snapshotDir,
		rywCache:    make(map[string]cacheEntry),
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
		case "SAVE":
			s.handleSave(conn, cmd)
		case "EXPIRE":
			s.handleExpire(conn, cmd)
		case "TTL":
			s.handleTtl(conn, cmd)
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

	s.cacheMutex.RLock()
	entry, found := s.rywCache[key]
	s.cacheMutex.RUnlock()

	var val string
	if found {
		if entry.expireAt > 0 && time.Now().UnixNano() > entry.expireAt {
			s.cacheMutex.Lock()
			delete(s.rywCache, key)
			s.cacheMutex.Unlock()
			val = "" // Treated as expired / not found
		} else {
			processedOffset, err := s.store.GetPartitionOffset(entry.partition)
			if err == nil && processedOffset >= entry.offset {
				s.cacheMutex.Lock()
				delete(s.rywCache, key)
				s.cacheMutex.Unlock()
				val = s.store.Get(key)
			} else {
				// Consumer lagging, serve from cache overlay
				val = entry.value
			}
		}
	} else {
		// key not found, fallback to rocksdb
		val = s.store.Get(key)
	}

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

	// CQRS Rule SET writes strictly to the Kafka WAL first
	wrappedVal := string(storage.WrapValue(val, 0))
	partition, offset, err := s.producer.Set(s.walTopic, key, wrappedVal)

	if err != nil {
		s.writeError(conn, fmt.Sprintf("ERR failed to commit to WAL: %v", err))
		return
	}
	s.cacheMutex.Lock()
	s.rywCache[key] = cacheEntry{
		value:     val,
		partition: partition,
		offset:    offset,
		expireAt:  0,
	}
	s.cacheMutex.Unlock()

	// Simple String OK
	conn.Write([]byte("+OK\r\n"))
}

func (s *Server) handleDel(conn net.Conn, cmd *Command) {
	if len(cmd.Args) != 1 {
		s.writeError(conn, "ERR wrong number of arguments for 'del' command")
		return
	}
	key := cmd.Args[0]
	val := s.store.Get(key)
	if val == "" {
		// Key does not exist, Return :0\r\n and skip Kafka to prevent WAL bloat
		conn.Write([]byte(":0\r\n"))
		return
	}

	// Tombstone record
	partition, offset, err := s.producer.Set(s.walTopic, key, "")
	if err != nil {
		s.writeError(conn, fmt.Sprintf("ERR failed to commit deletion to WAL: %v", err))
		return
	}
	s.cacheMutex.Lock()
	s.rywCache[key] = cacheEntry{
		value:     "",
		partition: partition,
		offset:    offset,
	}
	s.cacheMutex.Unlock()
	// Return 1 indicating 1 key was deleted
	conn.Write([]byte(":1\r\n"))
}

func (s *Server) handleSave(conn net.Conn, cmd *Command) {
	// Ensure the snapshots parent folder exists
	err1 := os.MkdirAll(s.snapshotDir, 0755)
	if err1 != nil {
		s.writeError(conn, fmt.Sprintf("ERR failed to bootstrap snapshots directory: %v", err1))
		return
	}
	unq := "/snapshot_" + strconv.FormatInt(time.Now().UnixMilli(), 10)
	snapPath := s.snapshotDir + unq
	err := s.store.CreateSnapshot(snapPath)
	if err != nil {
		s.writeError(conn,
			fmt.Sprintf("ERR failed to create snapshot: %v", err))
		return
	}

	conn.Write([]byte("+SNAPSHOT CREATED\r\n"))
}

func (s *Server) handleExpire(conn net.Conn, cmd *Command) {
	if len(cmd.Args) != 2 {
		s.writeError(conn, "ERR wrong number of arguments for 'expire' command")
		return
	}
	key := cmd.Args[0]
	seconds, err := strconv.ParseInt(cmd.Args[1], 10, 64)
	if err != nil {
		s.writeError(conn, "ERR value is not an integer or out of range")
		return
	}

	s.cacheMutex.RLock()
	entry, found := s.rywCache[key]
	s.cacheMutex.RUnlock()

	var currentVal string
	var hasKey bool

	if found {
		if entry.expireAt > 0 && time.Now().UnixNano() > entry.expireAt {
			s.cacheMutex.Lock()
			delete(s.rywCache, key)
			s.cacheMutex.Unlock()
			hasKey = false
		} else {
			if entry.value == "" {
				hasKey = false // deleted
			} else {
				currentVal = entry.value
				hasKey = true
			}
		}
	}

	if !hasKey {
		val, _, err := s.store.GetWithTTL(key)
		if err == nil && val != "" {
			currentVal = val
			hasKey = true
		}
	}

	if !hasKey {
		// Key does not exist
		conn.Write([]byte(":0\r\n"))
		return
	}

	// Calculate target UnixNano timestamp
	expireAt := time.Now().Add(time.Duration(seconds) * time.Second).UnixNano()

	wrappedVal := string(storage.WrapValue(currentVal, expireAt))

	// Commit to Kafka WAL
	partition, offset, err := s.producer.Set(s.walTopic, key, wrappedVal)
	if err != nil {
		s.writeError(conn, fmt.Sprintf("ERR failed to commit EXPIRE to WAL: %v", err))
		return
	}

	s.cacheMutex.Lock()
	s.rywCache[key] = cacheEntry{
		value:     currentVal,
		partition: partition,
		offset:    offset,
		expireAt:  expireAt,
	}
	s.cacheMutex.Unlock()

	conn.Write([]byte(":1\r\n"))
}

func (s *Server) handleTtl(conn net.Conn, cmd *Command) {
	if len(cmd.Args) != 1 {
		s.writeError(conn, "ERR wrong number of arguments for 'ttl' command")
		return
	}
	key := cmd.Args[0]

	s.cacheMutex.RLock()
	entry, found := s.rywCache[key]
	s.cacheMutex.RUnlock()

	if found {
		if entry.expireAt > 0 && time.Now().UnixNano() > entry.expireAt {
			s.cacheMutex.Lock()
			delete(s.rywCache, key)
			s.cacheMutex.Unlock()
		} else if entry.value == "" {
			conn.Write([]byte(":-2\r\n"))
			return
		} else {
			if entry.expireAt == 0 {
				conn.Write([]byte(":-1\r\n"))
			} else {
				remainingSecs := (entry.expireAt - time.Now().UnixNano()) / int64(time.Second)
				if remainingSecs < 0 {
					remainingSecs = 0
				}
				conn.Write([]byte(fmt.Sprintf(":%d\r\n", remainingSecs)))
			}
			return
		}
	}

	val, expireAt, err := s.store.GetWithTTL(key)
	if err != nil || val == "" {
		conn.Write([]byte(":-2\r\n"))
		return
	}

	if expireAt == 0 {
		conn.Write([]byte(":-1\r\n"))
	} else {
		remainingSecs := (expireAt - time.Now().UnixNano()) / int64(time.Second)
		if remainingSecs < 0 {
			remainingSecs = 0
		}
		conn.Write([]byte(fmt.Sprintf(":%d\r\n", remainingSecs)))
	}
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
