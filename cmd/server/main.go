package main

import (
	"io"
	"kvsdb/internal/config"
	"kvsdb/internal/server"
	"kvsdb/internal/storage"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// func to recursively copies a directory from src to dst
func copyDir(src string, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}

	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			if err := copyDir(srcPath, dstPath); err != nil {
				return err
			}
		} else {
			if err := copyFile(srcPath, dstPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func restoreLatestSnapshot(activeDir string, snapshotParentDir string) error {
	if entries, err := os.ReadDir(activeDir); err == nil && len(entries) > 0 {
		// activeDir exists and contains files , newer data so skip
		return nil
	}

	// Read snapshotParentDir if activeDir missing or empty
	entries, err := os.ReadDir(snapshotParentDir)
	if err != nil {
		return err
	}

	// Collect directories named snapshot_*
	var snapshots []string
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "snapshot_") {
			snapshots = append(snapshots, entry.Name())
		}
	}

	if len(snapshots) == 0 {
		return nil
	}

	// Sort alphabetically (same as chronological for unix millis names)
	sort.Strings(snapshots)

	latest := snapshots[len(snapshots)-1]
	latestSnapshotPath := filepath.Join(snapshotParentDir, latest)

	// Restore operation
	return copyDir(latestSnapshotPath, activeDir)
}

func main() {
	cfg := config.Load()
	err1 := restoreLatestSnapshot(cfg.RocksDBPath, cfg.SnapshotDir)
	if err1 != nil {
		log.Printf("Snapshot recovery skipped or failed: %v", err1)
	}

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

	s := server.NewServer(cfg.ServerAddr, m, producer, consumer, cfg.WalTopic, cfg.SnapshotDir)
	if err := s.Start(); err != nil {
		log.Fatalf("Server exited with error: %v", err)
	}
}
