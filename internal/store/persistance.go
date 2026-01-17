package store

import (
	"encoding/gob"
	"os"
	"sync"
)

type Snapshot struct {
	ArenaData []float32
	Dim       int
	Index     map[string]uint32
	RevIndex  []string
	Metadata  map[uint32][]byte
	NextIndex uint32
}

func (db *VectraDB) Save(filepath string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	snap := Snapshot{
		ArenaData: db.arena.data,
		Dim:       db.dim,
		Index:     db.index,
		RevIndex:  db.revIndex,
		Metadata:  db.metadata,
		NextIndex: db.arena.nextIndex,
	}

	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(snap); err != nil {
		return err
	}

	return nil
}

func LoadVectraDB(filepath string) (*VectraDB, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var snap Snapshot
	decoder := gob.NewDecoder(file)
	if err := decoder.Decode(&snap); err != nil {
		return nil, err
	}

	arena := &VectorArena{
		mu:        sync.Mutex{},
		data:      snap.ArenaData,
		dim:       snap.Dim,
		nextIndex: snap.NextIndex,
	}

	db := &VectraDB{
		arena:    arena,
		index:    snap.Index,
		revIndex: snap.RevIndex,
		metadata: snap.Metadata,
		dim:      snap.Dim,
	}

	return db, nil
}
