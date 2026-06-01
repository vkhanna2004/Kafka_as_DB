package config

import "os"

type Config struct {
	KafkaBrokers string
	WalTopic     string
	RocksDBPath  string
	ServerAddr   string
	GroupID      string
	SnapshotDir  string
}

// Load reads config from environment or returns sensible defaults
func Load() *Config {
	return &Config{
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
		WalTopic:     getEnv("KAFKA_WAL_TOPIC", "kvsdb-wal"),
		RocksDBPath:  getEnv("ROCKSDB_PATH", "tmp/rocksdb"),
		ServerAddr:   getEnv("SERVER_ADDR", ":6379"),
		GroupID:      getEnv("KAFKA_GROUP_ID", "kvsdb-wal-group"),
		SnapshotDir:  getEnv("SNAPSHOT_DIR", "tmp/snapshots"),
	}
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
