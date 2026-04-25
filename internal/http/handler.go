package http

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/hashicorp/raft"
	"github.com/rupamthxt/vectradb/internal/cluster"
	"github.com/rupamthxt/vectradb/internal/metrics"
	"github.com/rupamthxt/vectradb/internal/store"
)

type Handler struct {
	cluster *store.Cluster
}

type leaderAwareShard interface {
	Leader() *cluster.RaftNode
}

func NewHandler(cluster *store.Cluster) *Handler {
	return &Handler{cluster: cluster}
}

// Insert handles insert requests and adds a new vector record to the cluster
func (h *Handler) Insert(c *fiber.Ctx) error {
	var req InsertRequest

	metrics.InsertRequests.Inc()
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse json"})
	}

	if req.ID == "" || len(req.Vector) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "id and vector are required"})
	}

	timeNow := time.Now()
	err := h.cluster.Insert(req.ID, req.Vector, req.Data)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	metrics.InsertDuration.Observe(time.Since(timeNow).Seconds())
	metrics.TotalVectors.Inc()

	return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "data inserted successfully"})
}

// Search handles search requests and returns top K similar vectors from the database
func (h *Handler) Search(c *fiber.Ctx) error {
	var req SearchRequest

	metrics.SearchRequests.Inc()
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse json"})
	}

	if len(req.Vector) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "vector is required"})
	}

	if req.TopK <= 0 {
		req.TopK = 5 // Default TopK
	}

	timeNow := time.Now()
	results := h.cluster.Search(req.Vector, req.TopK)
	metrics.SearchDuration.Observe(time.Since(timeNow).Seconds())
	responseItems := make([]SearchResult, 0, len(results))
	for _, res := range results {
		var metaMap map[string]any
		if len(res.Data) > 0 {
			_ = json.Unmarshal(res.Data, &metaMap)
		}

		responseItems = append(responseItems, SearchResult{
			ID:    res.ID,
			Score: res.Score,
			Data:  metaMap,
		})
	}

	return c.JSON(SearchResponse{Results: responseItems})
}

// Delete handles delete requests and flags a vector with tombstone for deletion
func (h *Handler) Delete(c *fiber.Ctx) error {
	var req DeleteRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse json"})
	}

	if req.ID == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "id is missing"})
	}
	err := h.cluster.Delete(req.ID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	metrics.TotalVectors.Dec()
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "data deleted successfully"})
}

// Join handles join requests and returns a shard for the specific ID to be used by a new node for joining a cluster.
func (h *Handler) Join(c *fiber.Ctx) error {
	log.Printf("Reached1\n")
	var req JoinRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse json"})
	}
	shard := h.cluster.GetShardByID(req.ShardID)
	if shard == nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "shard not found"})
	}
	fmt.Printf("reached2")
	leaderShard, ok := shard.(leaderAwareShard)
	if !ok {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "shard does not support leader lookup"})
	}
	leader := leaderShard.Leader()
	if leader == nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "no leader available for shard"})
	}
	fmt.Printf("reached3")
	configFuture := leader.Raft.AddVoter(
		raft.ServerID(req.ServerID),
		raft.ServerAddress(req.Address),
		0,
		0,
	)
	fmt.Printf("reached4")
	if err := configFuture.Error(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	fmt.Printf("reached5")
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "node joined successfully"})
}
