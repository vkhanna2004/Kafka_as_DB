package storage

import (
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
)

type RocksStore struct {
	db           *grocksdb.DB
	dataCF       *grocksdb.ColumnFamilyHandle
	metaCF       *grocksdb.ColumnFamilyHandle
	readOptions  *grocksdb.ReadOptions
	writeOptions *grocksdb.WriteOptions
	mu           sync.RWMutex
}

func NewRocksStore(path string) (*RocksStore, error) {

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	cfNames := []string{
		"default",
		"meta",
	}

	cfOpts := []*grocksdb.Options{
		opts,
		opts,
	}

	db, handles, err := grocksdb.OpenDbColumnFamilies(opts, path, cfNames, cfOpts)
	if err != nil {
		log.Fatalf("Failed to open db with column families: %v", err)
	}

	store := &RocksStore{
		db:           db,
		dataCF:       handles[0],
		metaCF:       handles[1],
		readOptions:  grocksdb.NewDefaultReadOptions(),
		writeOptions: grocksdb.NewDefaultWriteOptions(),
		mu:           sync.RWMutex{},
	}

	return store, nil

}

func (r *RocksStore) Get(key string) string {
	slice, err := r.db.GetCF(r.readOptions, r.dataCF, []byte(key))
	if err != nil {
		log.Printf("RocksDB Get error: %v", err)
		return ""
	}
	defer slice.Free() // Free C++ memory

	if !slice.Exists() {
		return ""
	}

	val, expireAt := UnwrapValue(slice.Data())
	if expireAt > 0 && time.Now().UnixNano() > expireAt {
		// Key expired, delete it from disk
		_ = r.db.DeleteCF(r.writeOptions, r.dataCF, []byte(key))
		return ""
	}

	return val
}

func (r *RocksStore) Set(key, val string) {
	wrapped := WrapValue(val, 0)
	err := r.db.PutCF(r.writeOptions, r.dataCF, []byte(key), wrapped)
	if err != nil {
		log.Printf("RocksDB Set error: %v", err)
	}
}

func (r *RocksStore) PutWithOffset(key, value string, partition int32, offset int64) error {

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy() // Free C++ memory
	if value != "" {
		wb.PutCF(r.dataCF, []byte(key), []byte(value))
	} else {
		wb.DeleteCF(r.dataCF, []byte(key))
	}

	partitionKey := fmt.Sprintf("partition_%d_offset", partition)

	offsetStr := strconv.FormatInt(offset, 10)
	wb.PutCF(r.metaCF, []byte(partitionKey), []byte(offsetStr))

	err := r.db.Write(r.writeOptions, wb)
	if err != nil {
		return err
	}

	return nil
}

func (r *RocksStore) GetPartitionOffset(partition int32) (int64, error) {
	partitionKey := fmt.Sprintf("partition_%d_offset", partition)
	slice, err := r.db.GetCF(r.readOptions, r.metaCF, []byte(partitionKey))
	if err != nil {
		log.Printf("RocksDB Get Last Offset error: %v", err)
		return -1, err
	}
	defer slice.Free() // Free C++ memory

	if !slice.Exists() {
		return -1, nil
	}

	return strconv.ParseInt(string(slice.Data()), 10, 64)
}

func (r *RocksStore) Close() {
	r.dataCF.Destroy()
	r.metaCF.Destroy()
	r.readOptions.Destroy()
	r.writeOptions.Destroy()
	r.db.Close()
}

func (r *RocksStore) CreateSnapshot(destDir string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	checkpoint, err := r.db.NewCheckpoint()
	if err != nil {
		return fmt.Errorf("failed to initialize checkpoint: %w", err)
	}
	defer checkpoint.Destroy()
	return checkpoint.CreateCheckpoint(destDir, 0)
}

func (r *RocksStore) BeginWrite() {
	r.mu.RLock()
}
func (r *RocksStore) EndWrite() {
	r.mu.RUnlock()
}
func WrapValue(value string, expireAt int64) []byte {
	payload := make([]byte, 12+len(value))
	copy(payload[0:4], []byte("KVS1"))
	binary.BigEndian.PutUint64(payload[4:12], uint64(expireAt))
	copy(payload[12:], []byte(value))
	return payload
}

func UnwrapValue(data []byte) (string, int64) {
	if len(data) < 12 || string(data[0:4]) != "KVS1" {
		return string(data), 0 // Support legacy raw strings (no expiry)
	}
	expireAt := int64(binary.BigEndian.Uint64(data[4:12]))
	return string(data[12:]), expireAt
}

func (r *RocksStore) GetWithTTL(key string) (string, int64, error) {
	slice, err := r.db.GetCF(r.readOptions, r.dataCF, []byte(key))
	if err != nil {
		return "", 0, err
	}
	defer slice.Free()
	if !slice.Exists() {
		return "", 0, nil
	}
	val, expireAt := UnwrapValue(slice.Data())
	if expireAt > 0 && time.Now().UnixNano() > expireAt {
		_ = r.db.DeleteCF(r.writeOptions, r.dataCF, []byte(key))
		return "", 0, nil
	}
	return val, expireAt, nil
}

func (r *RocksStore) MultiGet(keys []string) []string {
	byteKeys := make([][]byte, len(keys))

	for i, k := range keys {
		byteKeys[i] = []byte(k)
	}
	slices, err := r.db.MultiGetCF(r.readOptions, r.dataCF, byteKeys...)
	if err != nil {
		log.Printf("RocksDB Get error: %v", err)
		return []string{}
	}
	defer slices.Destroy() // Free C++ memory

	if len(slices) == 0 {
		return []string{}
	}

	var ans []string
	for i, slice := range slices {
		if slice == nil {
			ans = append(ans, "")
			continue
		}
		val, expireAt := UnwrapValue(slice.Data())
		if expireAt > 0 && time.Now().UnixNano() > expireAt {
			// Key expired, delete it from disk
			_ = r.db.DeleteCF(r.writeOptions, r.dataCF, []byte(keys[i]))
			ans = append(ans, "")
			continue
		}
		ans = append(ans, val)
	}
	return ans
}
