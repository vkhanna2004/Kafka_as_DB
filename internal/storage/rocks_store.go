package storage

import (
	"fmt"
	"log"
	"strconv"

	"github.com/linxGnu/grocksdb"
)

type RocksStore struct {
	db           *grocksdb.DB
	dataCF       *grocksdb.ColumnFamilyHandle
	metaCF       *grocksdb.ColumnFamilyHandle
	readOptions  *grocksdb.ReadOptions
	writeOptions *grocksdb.WriteOptions
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

	return string(slice.Data())
}

func (r *RocksStore) Set(key, val string) {
	err := r.db.PutCF(r.writeOptions, r.dataCF, []byte(key), []byte(val))
	if err != nil {
		log.Printf("RocksDB Set error: %v", err)
	}
}

func (r *RocksStore) PutWithOffset(key, value string, partition int32, offset int64) error {

	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy() // Free C++ memory

	wb.PutCF(r.dataCF, []byte(key), []byte(value))

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
