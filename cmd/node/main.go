package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/rupamthxt/vectradb/internal/cluster"
	"github.com/rupamthxt/vectradb/internal/store"
)

func main() {
	fmt.Println("Initializing node.....")

	bootstrap := flag.Bool("bootstrap", false, "Bootstrap the node as leader")
	numShards := flag.Int("shards", 3, "Number of concurrent shards")
	raftPort := flag.Int("raft-port", 9000, "Port for the raft node")
	flag.Parse()

	const baseDir = "app/data"
	os.MkdirAll(baseDir, 0755)

	for i := range *numShards {
		var nodes []*cluster.RaftNode
		nodeId := fmt.Sprintf("node_%x", time.Now().UnixNano())
		nodeDir := fmt.Sprintf("%s/shard_%d/%s", baseDir, i, nodeId)
		os.MkdirAll(nodeDir, 0755)

		db, err := store.NewVectraDB(128, nodeDir)
		if err != nil {
			log.Fatalf("Error creating database %v", err)
		}
		raftPort := *raftPort + i
		raftNode, err := cluster.NewRaftNode(i, nodeId, baseDir, raftPort, db)
		if err != nil {
			log.Fatalf("Error creating raft node %v", err)
		}
		nodes = append(nodes, raftNode)
	}

}
