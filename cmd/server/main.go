package main

import (
	"kvsdb/internal/server"
	"kvsdb/internal/storage"
	"log"
)

func main() {
	// m := &storage.MemoryStore{}
	m, err := storage.NewRocksStore("tmp/rocksdb")
	if err != nil {
		log.Fatalf("Failed to open RocksDB: %v", err)
	}
	defer m.Close() // Keep C++ allocations clean

	producer, err := storage.InitializeKafkaProducer()
	if err == nil {
		producer.Set("kvsdb-wal", "key1", "msg1")
		producer.Set("kvsdb-wal", "key2", "msg2")
		producer.Set("kvsdb-wal", "key3", "msg3")
		producer.Set("kvsdb-wal", "key1", "")
	}

	consumer, err2 := storage.InitializeKafkaConsumer(m)
	if err2 == nil {
		consumer.Subscribe("kvsdb-wal")
		go consumer.Poll() //run in background
	}

	s := server.NewServer(":6379", m, producer)
	s.Start()
	// time.Sleep(5 * time.Second)

	// fmt.Println("Value for key1 is:", m.Get("key1"))
	// fmt.Println("Value for key2 is:", m.Get("key2"))

}
