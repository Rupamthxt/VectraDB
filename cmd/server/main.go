package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rupamthxt/vectradb/internal/cluster"
	"github.com/rupamthxt/vectradb/internal/store"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/gofiber/fiber/v2/middleware/logger"

	vectorHttp "github.com/rupamthxt/vectradb/internal/http"
)

const SnapshotPath = "./vectradb.snap"

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

func (s *shardGroup) Delete(id string) error {
	for _, n := range s.nodes {
		if n.Raft.State() == raft.Leader {
			return n.Delete(id)
		}
	}
	return fmt.Errorf("no leader for shard")
}

func (s *shardGroup) Leader() *cluster.RaftNode {
	for _, n := range s.nodes {
		if n.Raft.State() == raft.Leader {
			return n
		}
	}
	return nil
}

func main() {
	fmt.Println("Initializing VectraDB (High-Perf) mode...")

	// bootstrap := flag.Bool("bootstrap", false, "Bootstrap the cluster (Leader only)")
	raftPort := flag.Int("raft-port", 9000, "Port for the raft node")
	// httpPort := flag.Int("http-port", 8080, "Port for the HTTP server")
	// joinAddr := flag.String("join", "", "Address of the already running service to join to")
	// nodeID := flag.String("node-id", "node1", "Unique ID for this node")
	numShards := flag.Int("shards", 3, "The total number of shards of the database")

	flag.Parse()

	const baseDir = "app/data"
	os.MkdirAll(baseDir, 0755)

	var shards []store.ShardHandler

	for i := 0; i < *numShards; i++ {
		// Create a 1-node Raft cluster for each shard
		const nodesPerShard = 3
		var nodes []*cluster.RaftNode

		for n := 0; n < nodesPerShard; n++ {
			nodeDir := fmt.Sprintf("%s/shard_%d/node_%d", baseDir, i, n)
			os.MkdirAll(nodeDir, 0755)

			db, err := store.NewVectraDB(128, nodeDir)
			if err != nil {
				log.Fatalf("failed to create db for shard %d node %d: %v", i, n, err)
			}

			raftPort := *raftPort + i*10 + n
			nodeID := fmt.Sprintf("node_%d", n)
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
				ID:      raft.ServerID(fmt.Sprintf("node_%d", n)),
				Address: raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", *raftPort+i*10+n)),
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

	app := fiber.New()
	app.Use(logger.New())

	handler := vectorHttp.NewHandler(c)
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	api := app.Group("/api/v1")
	api.Post("/insert", handler.Insert)
	api.Post("/search", handler.Search)
	api.Post("/delete", handler.Delete)

	log.Println("VectraDB listening on port : 8080")
	log.Fatal(app.Listen(":8080"))
}
