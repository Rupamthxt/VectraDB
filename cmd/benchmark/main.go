package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"log"
	"net/http"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rupamthxt/vectradb/internal/cluster"
	"github.com/rupamthxt/vectradb/internal/metrics"
	"github.com/rupamthxt/vectradb/internal/store"
)

const (
	RaftBasePort = 19000
)

var (
	dimension    = 128
	totalVectors = 5_00 // default, overridden via flags
	numQueries   = 1000
	numShards    = 1
	metricsPort  = 9091
)

type shardGroup struct {
	nodes []*cluster.RaftNode
}

func (s *shardGroup) Insert(id string, vector []float32, data any) error {
	for _, n := range s.nodes {
		if n.Raft.State() == raft.Leader {
			return n.Insert(id, vector, data)
		}
	}
	return fmt.Errorf("no leader for shard")
}

func (s *shardGroup) Search(query []float32, topK int) []store.VectroRecord {
	// Read from the first available node (may be eventual) or the leader if known.
	for _, n := range s.nodes {
		if n.Raft.State() == raft.Leader {
			return n.Search(query, topK)
		}
	}
	if len(s.nodes) > 0 {
		return s.nodes[0].Search(query, topK)
	}
	return nil
}

func main() {
	// allow customization via flags
	dimPtr := flag.Int("dim", dimension, "vector dimension")
	itemsPtr := flag.Int("items", totalVectors, "total vectors to insert")
	queriesPtr := flag.Int("queries", numQueries, "number of search queries")
	shardsPtr := flag.Int("shards", numShards, "number of raft shards")
	metricsPtr := flag.Int("metrics-port", metricsPort, "port for Prometheus metrics")
	flag.Parse()

	dimension = *dimPtr
	totalVectors = *itemsPtr
	numQueries = *queriesPtr
	numShards = *shardsPtr
	metricsPort = *metricsPtr

	fmt.Println("🔥 Starting VectraDB Distributed Benchmark (Raft + IVF)")
	fmt.Printf("Config: Dim=%d | Items=%d | Shards=%d\n", dimension, totalVectors, numShards)

	baseDir := "data_bench"
	// os.RemoveAll(baseDir)
	os.MkdirAll(baseDir, 0755)
	// defer os.RemoveAll(baseDir)

	var shards []store.ShardHandler

	fmt.Println("⚡ Initializing Raft Groups...")

	// start prometheus metrics endpoint
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		addr := fmt.Sprintf(":%d", metricsPort)
		log.Printf("metrics listening on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Fatalf("metrics server failed: %v", err)
		}
	}()

	for i := 0; i < numShards; i++ {
		// Create a 1-node Raft cluster for each shard
		const nodesPerShard = 3
		var nodes []*cluster.RaftNode

		for n := 0; n < nodesPerShard; n++ {
			nodeDir := fmt.Sprintf("%s/shard_%d/node_%d", baseDir, i, n)
			os.MkdirAll(nodeDir, 0755)

			db, err := store.NewVectraDB(dimension, nodeDir)
			if err != nil {
				log.Fatalf("failed to create db for shard %d node %d: %v", i, n, err)
			}

			raftPort := RaftBasePort + i*10 + n
			nodeID := fmt.Sprintf("bench_node-shard-%d-node-%d", i, n)
			raftNode, err := cluster.NewRaftNode(i, nodeID, baseDir, raftPort, db)
			if err != nil {
				log.Fatalf("failed to create raft node for shard %d node %d: %v", i, n, err)
			}
			nodes = append(nodes, raftNode)
		}

		// Bootstrap the shard's raft cluster (first node bootstraps; others join as voters).
		servers := make([]raft.Server, 0, nodesPerShard)
		for n := 0; n < nodesPerShard; n++ {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(fmt.Sprintf("bench_node-shard-%d-node-%d", i, n)),
				Address: raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", RaftBasePort+i*10+n)),
			})
		}

		// Bootstrap this shard as a single-node cluster first.
		// Later we will add the remaining nodes as voters.
		bootstrapCfg := raft.Configuration{Servers: []raft.Server{servers[0]}}

		confFut := nodes[0].Raft.GetConfiguration()
		if err := confFut.Error(); err != nil {
			log.Printf("warning: could not read raft configuration for shard %d: %v", i, err)
		} else if len(confFut.Configuration().Servers) == 0 {
			bootFut := nodes[0].Raft.BootstrapCluster(bootstrapCfg)
			if err := bootFut.Error(); err != nil && err != raft.ErrCantBootstrap {
				log.Fatalf("failed to bootstrap raft cluster for shard %d: %v", i, err)
			}
		}

		// Wait briefly for node0 to become leader so we can add voters.
		for r := 0; r < 10; r++ {
			if nodes[0].Raft.State() == raft.Leader {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		for n := 1; n < nodesPerShard; n++ {
			// Retry add voter until we either succeed or decide it cannot be done.
			for r := 0; r < 5; r++ {
				err := nodes[0].Raft.AddVoter(servers[n].ID, servers[n].Address, 0, 0).Error()
				if err == nil {
					break
				}
				if err == raft.ErrNotLeader {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				// Some clusters may already have the voter configured.
				if err == raft.ErrCantBootstrap || err == raft.ErrNotVoter {
					break
				}
				log.Printf("warning: could not add voter for shard %d node %d: %v", i, n, err)
				break
			}
		}

		shards = append(shards, &shardGroup{nodes: nodes})
	}

	time.Sleep(3 * time.Second) // Wait for elections
	c := store.NewCluster(shards)

	// --- Phase 1: Ingestion ---
	fmt.Println("\n--- Phase 1: Ingestion (Raft Log Replication) ---")
	start := time.Now()

	// Use 10 concurrent workers for ingestion
	var wg sync.WaitGroup
	workers := 10
	batch := totalVectors / workers
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			offset := idx * batch
			for i := 0; i < batch; i++ {
				metrics.InsertRequests.Inc()
				startIns := time.Now()
				c.Insert(fmt.Sprintf("vec-%d", offset+i), randomVector(dimension), nil)
				metrics.InsertDuration.Observe(time.Since(startIns).Seconds())
				metrics.TotalVectors.Inc()
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("✅ Ingestion Complete: %.2fs\n", time.Since(start).Seconds())
	iqps := float64(totalVectors) / time.Since(start).Seconds()
	fmt.Printf("🚀 Ingestion QPS: %.2f\n", iqps)

	// --- Phase 2: Search ---
	fmt.Println("\n--- Phase 2: Search (HNSW) ---")
	startSearch := time.Now()
	wgSearch := sync.WaitGroup{}
	wgSearch.Add(numQueries)

	for i := 0; i < numQueries; i++ {
		go func() {
			defer wgSearch.Done()
			metrics.SearchRequests.Inc()
			startSearchLoop := time.Now()
			c.Search(randomVector(dimension), 10)
			metrics.SearchDuration.Observe(time.Since(startSearchLoop).Seconds())
		}()
	}
	wgSearch.Wait()

	qps := float64(numQueries) / time.Since(startSearch).Seconds()
	fmt.Printf("🚀 HNSW QPS: %.2f\n", qps)

	// keep process running so prometheus can scrape metrics after benchmark completes
	// fmt.Println("🔋 benchmark complete – metrics remain available at :9091/metrics until you stop the program")
	// select{}
}

func randomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = rand.Float32()
	}
	return vec
}
