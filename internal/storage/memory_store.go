package storage

import (
	"fmt"
	"sync"
)

type Engine interface {
	Get(key string) string
	Set(key, val string)
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
