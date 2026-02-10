package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/rupamthxt/vectradb/internal/store"
)

const (
	RaftTimeout = 10 * time.Second
)

type RaftNode struct {
	Raft *raft.Raft
	FSM  *FSM
	// we keep a reference to the database for read only operations
	DB *store.VectraDB
}

func NewRaftNode(shardID int, nodeID string, baseDir string, raftPort int, db *store.VectraDB) (*RaftNode, error) {
	fsm := NewFSM(db)

	raftDir := filepath.Join(baseDir, fmt.Sprintf("shard_%d", shardID), "raft")
	os.MkdirAll(raftDir, 0755)

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(fmt.Sprintf("%s-shard-%d", nodeID, shardID))

	addr := fmt.Sprintf("127.0.0.1:%d", raftPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(addr, tcpAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "logs.dat"))
	if err != nil {
		return nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.dat"))
	if err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, 1, os.Stderr)
	if err != nil {
		return nil, err
	}

	raftNode, err := raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	return &RaftNode{
		Raft: raftNode,
		FSM:  fsm,
		DB:   db,
	}, nil
}

func (rn *RaftNode) Insert(id string, vector []float32, data interface{}) error {
	if rn.Raft.State() != raft.Leader {
		return fmt.Errorf("not the leader of this shard")
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %v", err)
	}
	cmd := Command{
		Op:     "insert",
		Id:     id,
		Vector: vector,
		Data:   json.RawMessage(jsonData),
	}

	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	future := rn.Raft.Apply(b, RaftTimeout)
	if err := future.Error(); err != nil {
		return err
	}

	if fsmErr, ok := future.Response().(error); ok {
		return fsmErr
	}
	return nil
}

func (rn *RaftNode) Search(query []float32, topK int) []store.VectroRecord {
	return rn.DB.Search(query, topK)
}

func (rn *RaftNode) TrainIndex() error {
	rn.DB.CreateIndex()
	return nil
}
