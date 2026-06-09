package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
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
	MultiGet(keys []string) []string
	Scan(prefix string, cursor string, count int) ([]string, string, error)
	GetProperty(name string) string
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
	if strings.HasPrefix(val, "MSET_BATCH:") {
		jsonPayload := val[len("MSET_BATCH:"):]
		var mutations []Mutation
		if err := json.Unmarshal([]byte(jsonPayload), &mutations); err != nil {
			return err
		}
		for _, mut := range mutations {
			if mut.Val != "" {
				m.Set(mut.Key, mut.Val)
			} else {
				m.data.Delete(mut.Key)
			}
		}
		return nil
	}
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

func (m *MemoryStore) MultiGet(keys []string) []string {
	values := make([]string, len(keys))
	for i, key := range keys {
		if value, ok := m.data.Load(key); ok {
			values[i] = value.(string)
		} else {
			values[i] = ""
		}
	}
	return values
}
func (m *MemoryStore) Scan(prefix string, cursor string, count int) ([]string, string, error) {
	var keys []string
	m.data.Range(func(k, v interface{}) bool {
		keyStr := k.(string)
		if strings.HasPrefix(keyStr, prefix) {
			keys = append(keys, keyStr)
		}
		return true
	})
	sort.Strings(keys)

	// Find starting offset using cursor
	startIdx := 0
	if cursor != "" {
		for idx, k := range keys {
			if k == cursor {
				startIdx = idx + 1
				break
			}
		}
	}

	// Extract the page
	var results []string
	nextCursor := ""
	for i := startIdx; i < len(keys); i++ {
		if len(results) >= count {
			nextCursor = keys[i-1] // Last returned key is the next cursor
			break
		}
		results = append(results, keys[i])
	}
	return results, nextCursor, nil
}

func (m *MemoryStore) GetProperty(name string) string {
	return ""
}
