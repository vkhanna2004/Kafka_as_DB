package main

import (
	"kvsdb/internal/config"
	"kvsdb/internal/server"
	"kvsdb/internal/storage"
	"log"
)

func main() {
	cfg := config.Load()
	// m := &storage.MemoryStore{}
	m, err := storage.NewRocksStore(cfg.RocksDBPath)
	if err != nil {
		log.Fatalf("Failed to open RocksDB: %v", err)
	}
	defer m.Close() // Keep C++ allocations clean

	producer, err := storage.InitializeKafkaProducer(cfg.KafkaBrokers)
	if err == nil {
		producer.Set(cfg.WalTopic, "key1", "msg1")
		producer.Set(cfg.WalTopic, "key2", "msg2")
		producer.Set(cfg.WalTopic, "key3", "msg3")
		producer.Set(cfg.WalTopic, "key1", "")
	}

	consumer, err2 := storage.InitializeKafkaConsumer(cfg.KafkaBrokers, cfg.GroupID, m)
	if err2 == nil {
		consumer.Subscribe(cfg.WalTopic)
		go consumer.Poll() // run in background
	}

	s := server.NewServer(cfg.ServerAddr, m, producer, cfg.WalTopic)
	if err := s.Start(); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}
