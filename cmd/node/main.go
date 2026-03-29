package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rupamthxt/vectradb/internal/cluster"
	"github.com/rupamthxt/vectradb/internal/store"

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

func main() {
	fmt.Println("Initializing node.....")

	bootstrap := flag.Bool("bootstrap", false, "Bootstrap the node as leader")
	numShards := flag.Int("shards", 3, "Number of concurrent shards")
	raftPort := flag.Int("raft-port", 9000, "Port for the raft node")
	flag.Parse()

	const baseDir = "app/data"
	os.MkdirAll(baseDir, 0755)

	var shards []store.ShardHandler

	if *bootstrap {
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

			bootstrapConfig := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(nodeId),
						Address: raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", raftPort+i)),
					},
				},
			}

			confFut := nodes[0].Raft.GetConfiguration()
			if err := confFut.Error(); err != nil {
				log.Fatalf("Error getting raft configuration: %v", err)
			} else if len(confFut.Configuration().Servers) == 0 {
				bootFut := nodes[0].Raft.BootstrapCluster(bootstrapConfig)
				if err := bootFut.Error(); err != nil && err != raft.ErrCantBootstrap {
					log.Fatalf("failed to bootstrap raft cluster for shard %d: %v", i, err)
				}
			}

			for r := 0; r < 10; r++ {
				if nodes[0].Raft.State() == raft.Leader {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}

			shards = append(shards, &shardGroup{nodes: nodes})

		}
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
