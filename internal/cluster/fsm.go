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

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	switch cmd.Op {
	case "insert":
		return f.db.Insert(cmd.Id, cmd.Vector, cmd.Data)
	default:
		return fmt.Errorf("unknown command: %s", cmd.Op)
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &NoOpSnapshot{}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	// Implement restore logic if needed
	return nil
}

// Dummy struct for snapshot
type NoOpSnapshot struct{}

func (s *NoOpSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }

func (s *NoOpSnapshot) Release() {}
