package http

import (
	"encoding/json"

	"github.com/gofiber/fiber/v2"
	"github.com/rupamthxt/vectradb/internal/store"
)

type Handler struct {
	cluster *store.Cluster
}

func NewHandler(cluster *store.Cluster) *Handler {
	return &Handler{cluster: cluster}
}

// Insert handles insert requests and adds a new vector record to the cluster
func (h *Handler) Insert(c *fiber.Ctx) error {
	var req InsertRequest

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse json"})
	}

	if req.ID == "" || len(req.Vector) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "id and vector are required"})
	}

	err := h.cluster.Insert(req.ID, req.Vector, req.Data)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "data inserted successfully"})
}

// Search handles search requests and returns top K similar vectors from the database
func (h *Handler) Search(c *fiber.Ctx) error {
	var req SearchRequest

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse json"})
	}

	if len(req.Vector) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "vector is required"})
	}

	if req.TopK <= 0 {
		req.TopK = 5 // Default TopK
	}

	results := h.cluster.Search(req.Vector, req.TopK)

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
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "data deleted successfully"})
}

// Join handles join requests and returns a shard for the specific ID to be used by a new node for joining a cluster.
func (h *Handler) Join(c *fiber.Ctx) (store.ShardHandler, error) {
	var req JoinRequest
	if err := c.BodyParser(&req); err != nil {
		return nil, c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse json"})
	}
	s := h.cluster.GetShardByID(req.ShardID)
	return s, nil
}
