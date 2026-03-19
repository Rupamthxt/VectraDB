package store

import (
	"encoding/json"
	"fmt"
	"sync"
)

type VectroRecord struct {
	ID    string
	Score float32
	Data  json.RawMessage
}

type VectraDB struct {
	mu       sync.RWMutex
	index    map[string]uint32
	revIndex []string

	// Hot Path Storage
	Arena *VectorArena

	// Cold Path Storage
	metaLocs map[uint32]FileLocation

	disk *DiskStore

	dim int

	HNSW *HNSWIndex
}

func NewVectraDB(dim int, storagePath string) (*VectraDB, error) {

	ds, err := NewDiskStore(fmt.Sprintf("%s/data.bin", storagePath))
	if err != nil {
		return nil, fmt.Errorf("Failed to init disk store at %s: %w", storagePath, err)
	}
	localArena := NewVectorArena(dim)

	db := &VectraDB{
		index:    make(map[string]uint32),
		revIndex: make([]string, 10000),
		Arena:    localArena,
		metaLocs: make(map[uint32]FileLocation),
		disk:     ds,
		dim:      dim,
		HNSW:     NewHNSWIndex(localArena),
	}

	return db, nil
}

func (db *VectraDB) Insert(id string, vector []float32, data any) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("Failed to marshal metadata: %w", err)
	}

	idx, err := db.Arena.Add(vector)
	if err != nil {
		return err
	}

	loc, err := db.disk.Write(bytes)
	if err != nil {
		return err
	}

	db.index[id] = idx
	db.revIndex = append(db.revIndex, id)

	db.metaLocs[idx] = loc

	db.HNSW.Add(vector, id, idx)

	return nil
}

func (db *VectraDB) Get(id string) ([]float32, []byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	idx, exists := db.index[id]
	if !exists {
		return nil, nil, false
	}

	vec, _ := db.Arena.Get(idx)
	metaLoc := db.metaLocs[idx]
	meta, err := db.disk.Read(metaLoc)
	if err != nil {
		return vec.Dequantize(), nil, true
	}
	return vec.Dequantize(), meta, true
}

func (db *VectraDB) Search(query []float32, topK int) []VectroRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.HNSW.Search(query, topK)

}

func (db *VectraDB) Delete(id string) error {
	return db.HNSW.Delete(id)
}

func (db *VectraDB) InsertInMemory(id string, vector []float32) (uint32, error) {
	idx, err := db.Arena.Add(vector)

	db.index[id] = idx
	db.revIndex[idx] = id
	// db.metaLocs[idx] = loc
	return idx, err
}
