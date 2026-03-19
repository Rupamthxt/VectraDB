package cluster

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"github.com/rupamthxt/vectradb/internal/store"
)

// Command is what we replicate across the network
type Command struct {
	Op     string          `json:"op"` // Insert, Delete
	Id     string          `json:"id"`
	Vector []float32       `json:"vector"`
	Data   json.RawMessage `json:"data"`
}

type FSM struct {
	db *store.VectraDB
}

func NewFSM(db *store.VectraDB) *FSM {
	return &FSM{db: db}
}

// Apply applies a Raft Log Entry to the FSM.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Op {
	case "insert":
		return f.db.Insert(cmd.Id, cmd.Vector, cmd.Data)
	case "delete":
		f.db.HNSW.Delete(cmd.Id)
		return nil
	default:
		return fmt.Errorf("unknown command: %s", cmd.Op)
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	// Obtain a read lock on the HNSW index to ensure consistency during snapshot
	f.db.HNSW.RLock()
	defer f.db.HNSW.RUnlock()

	var records []VectorRecord

	for id, nodes := range f.db.HNSW.Nodes {

		// If node is tombstone, just skip it. We won't write it in snapshot.
		if f.db.HNSW.Tombstones[id] {
			continue
		}
		vec, err := f.db.Arena.Get(nodes.ArenaOffset)
		if err == nil {
			records = append(records, VectorRecord{
				ID:     id,
				Vector: vec.Dequantize(),
			})
		}
	}

	return &fsmSnapshot{records: records}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var records []VectorRecord
	if err := json.NewDecoder(rc).Decode(&records); err != nil {
		return err
	}

	for _, record := range records {
		// arenaOffset, err := f.db.Arena.Add(record.Vector)
		arenaOffset, err := f.db.InsertInMemory(record.ID, record.Vector)
		if err != nil {
			return err
		}
		f.db.HNSW.Add(record.Vector, record.ID, arenaOffset)
	}
	return nil
}
