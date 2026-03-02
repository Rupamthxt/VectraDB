package store

import (
	"encoding/json"
	"fmt"

	// "math"

	// "sort"
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
	arena *VectorArena

	// Cold Path Storage
	metaLocs map[uint32]FileLocation

	disk *DiskStore

	dim int

	wal *WAL

	hnsw *HNSWIndex
}

func NewVectraDB(dim int, storagePath string) (*VectraDB, error) {

	ds, err := NewDiskStore(storagePath)
	if err != nil {
		return nil, fmt.Errorf("Failed to init disk store at %s: %w", storagePath, err)
	}
	wal, err := OpenWal(storagePath)
	if err != nil {
		return nil, err
	}
	localArena := NewVectorArena(dim)

	db := &VectraDB{
		index:    make(map[string]uint32),
		revIndex: make([]string, 10000),
		arena:    localArena,
		metaLocs: make(map[uint32]FileLocation),
		disk:     ds,
		dim:      dim,
		wal:      wal,
		hnsw:     NewHNSWIndex(localArena),
	}

	fmt.Println("Replaying WAL to restore data....")
	count := 0
	err = wal.Recover(func(id string, vector []float32, meta []byte, loc FileLocation) {
		db.insertInMemory(id, vector, loc)
		count++
	})
	fmt.Printf("Recovered %d records from WAL\n", count)

	return db, nil
}

func (db *VectraDB) Insert(id string, vector []float32, data any) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("Failed to marshal metadata: %w", err)
	}

	idx, err := db.arena.Add(vector)
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

	db.hnsw.Add(vector, id, idx)

	return nil
}

func (db *VectraDB) insertInMemory(id string, vector []float32, loc FileLocation) error {
	idx, err := db.arena.Add(vector)

	db.index[id] = idx
	db.revIndex[idx] = id
	db.metaLocs[idx] = loc
	return err
}

func (db *VectraDB) Get(id string) ([]float32, []byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	idx, exists := db.index[id]
	if !exists {
		return nil, nil, false
	}

	vec, _ := db.arena.Get(idx)
	metaLoc := db.metaLocs[idx]
	meta, err := db.disk.Read(metaLoc)
	if err != nil {
		return vec, nil, true
	}
	return vec, meta, true
}

func (db *VectraDB) Search(query []float32, topK int) []VectroRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()

	return db.hnsw.Search(query, topK)

}

// func cosineSimilarity(a, b []float32) float32 {
// 	var dot, mag1, mag2 float32
// 	for i := range a {
// 		dot += a[i] * b[i]
// 		mag1 += a[i] * a[i]
// 		mag2 += b[i] * b[i]
// 	}
// 	if mag1 == 0 || mag2 == 0 {
// 		return 0
// 	}
// 	return dot / (float32(math.Sqrt(float64(mag1)))) * float32(math.Sqrt(float64(mag2)))

// }
