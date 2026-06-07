package storage

import (
	"fmt"
	"sync"
)

type Engine interface {
	Get(key string) string
	Set(key, val string)
	PutWithOffset(key, val string, partition int32, offset int64) error
	GetPartitionOffset(partition int32) (int64, error)
	Close()
	CreateSnapshot(destDir string) error
	BeginWrite()
	EndWrite()
	GetWithTTL(key string) (string, int64, error)
}

type MemoryStore struct {
	data sync.Map //handles locking internally
}

func (m *MemoryStore) Get(key string) string {
	value, ok := m.data.Load(key)
	if !ok {
		fmt.Println("line 21")
		return ""
	}
	return value.(string)
}

func (m *MemoryStore) Set(key, val string) {
	m.data.Store(key, val)
}

func (m *MemoryStore) PutWithOffset(key, val string, partition int32, offset int64) error {
	m.Set(key, val)
	return nil
}

func (m *MemoryStore) GetPartitionOffset(int32) (int64, error) {
	return -1, nil
}

func (m *MemoryStore) Close()      {}
func (m *MemoryStore) BeginWrite() {}
func (m *MemoryStore) EndWrite()   {}

func (m *MemoryStore) CreateSnapshot(destDir string) error {
	return nil // no operatioon for in-memory
}

func (m *MemoryStore) GetWithTTL(key string) (string, int64, error) {
	val := m.Get(key)
	if val == "" {
		return "", 0, nil
	}
	return val, 0, nil // MemoryStore has no expiry
}
