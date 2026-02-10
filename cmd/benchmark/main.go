package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rupamthxt/vectradb/internal/cluster"
	"github.com/rupamthxt/vectradb/internal/store"
)

const (
	Dimension    = 128
	TotalVectors = 5_000_000 // Keeping it small for Raft ingest speed
	NumQueries   = 1000
	NumShards    = 3
	RaftBasePort = 19000
)

func main() {
	fmt.Println("🔥 Starting VectraDB Distributed Benchmark (Raft + IVF)")
	fmt.Printf("Config: Dim=%d | Items=%d | Shards=%d\n", Dimension, TotalVectors, NumShards)

	baseDir := "data_bench"
	os.RemoveAll(baseDir)
	os.MkdirAll(baseDir, 0755)
	defer os.RemoveAll(baseDir)

	var shards []store.ShardHandler
	nodeID := "bench_node"

	fmt.Println("⚡ Initializing Raft Groups...")
	for i := 0; i < NumShards; i++ {
		shardDir := fmt.Sprintf("%s/shard_%d", baseDir, i)
		os.MkdirAll(shardDir, 0755)

		dbPath := fmt.Sprintf("%s/meta.bin", shardDir)
		db, _ := store.NewVectraDB(Dimension, dbPath)

		raftPort := RaftBasePort + i
		raftNode, _ := cluster.NewRaftNode(i, nodeID, baseDir, raftPort, db)

		cfg := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(fmt.Sprintf("%s-shard-%d", nodeID, i)),
					Address: raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", raftPort)),
				},
			},
		}
		raftNode.Raft.BootstrapCluster(cfg)
		shards = append(shards, raftNode)
	}

	time.Sleep(3 * time.Second) // Wait for elections
	c := store.NewCluster(shards)

	// --- Phase 1: Ingestion ---
	fmt.Println("\n--- Phase 1: Ingestion (Raft Log Replication) ---")
	start := time.Now()

	// Use 10 concurrent workers for ingestion
	var wg sync.WaitGroup
	workers := 10
	batch := TotalVectors / workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			offset := idx * batch
			for i := 0; i < batch; i++ {
				c.Insert(fmt.Sprintf("vec-%d", offset+i), randomVector(Dimension), nil)
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("✅ Ingestion Complete: %.2fs\n", time.Since(start).Seconds())

	// --- Phase 2: Training ---
	fmt.Println("\n--- Phase 2: Training Distributed IVF Index ---")
	startTrain := time.Now()

	// This triggers parallel training on all shards
	c.TrainIndex()

	fmt.Printf("✅ Index Trained in %s\n", time.Since(startTrain))

	// --- Phase 3: Search ---
	fmt.Println("\n--- Phase 3: Search (Distributed IVF) ---")
	startSearch := time.Now()
	wgSearch := sync.WaitGroup{}
	wgSearch.Add(NumQueries)

	for i := 0; i < NumQueries; i++ {
		go func() {
			defer wgSearch.Done()
			c.Search(randomVector(Dimension), 10)
		}()
	}
	wgSearch.Wait()

	qps := float64(NumQueries) / time.Since(startSearch).Seconds()
	fmt.Printf("🚀 Distributed IVF QPS: %.2f\n", qps)
}

func randomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = rand.Float32()
	}
	return vec
}
