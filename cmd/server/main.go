package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/hashicorp/raft"
	"github.com/rupamthxt/vectradb/internal/cluster"
	"github.com/rupamthxt/vectradb/internal/store"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"

	vectorHttp "github.com/rupamthxt/vectradb/internal/http"
)

const SnapshotPath = "./vectradb.snap"

func main() {
	fmt.Println("Initializing VectraDB (High-Perf) mode...")

	bootstrap := flag.Bool("bootstrap", false, "Bootstrap the cluster (Leader only)")

	flag.Parse()

	nodeID := "node1"
	basePort := 9000
	numShards := 3

	var shards []store.ShardHandler

	for i := range numShards {
		dbPath := fmt.Sprintf("data/%s/shard_%d/meta.bin", nodeID, i)
		db, _ := store.NewVectraDB(128, dbPath)

		raftNode, _ := cluster.NewRaftNode(i, nodeID, "data/"+nodeID, basePort+i, db)

		if *bootstrap {
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      raft.ServerID(fmt.Sprintf("%s-shard-%d", nodeID, i)),
						Address: raft.ServerAddress(fmt.Sprintf("127.0.0.1:%d", basePort+i)),
					},
				},
			}
			raftNode.Raft.BootstrapCluster(configuration)
		}
		shards = append(shards, raftNode)
	}

	c := store.NewCluster(shards)

	app := fiber.New()
	app.Use(logger.New())

	handler := vectorHttp.NewHandler(c)

	api := app.Group("/api/v1")
	api.Post("/insert", handler.Insert)
	api.Post("/search", handler.Search)

	admin := app.Group("/admin")
	admin.Post("/index", handler.CreateIndex)

	log.Println("VectraDB listening on port : 8080")
	log.Fatal(app.Listen(":8080"))
}
