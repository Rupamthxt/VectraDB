package cluster

import (
	"encoding/json"

	"github.com/hashicorp/raft"
)

// Represents the data we are saving
type VectorRecord struct {
	ID     string    `json:"id"`
	Vector []float32 `json:"vector"`
}

// Implements raft.FSMSnapshot interface
type fsmSnapshot struct {
	records []VectorRecord
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		encoder := json.NewEncoder(sink)
		if err := encoder.Encode(s.records); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (s *fsmSnapshot) Release() {}
