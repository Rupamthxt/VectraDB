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

//	func (h *Handler) SaveToDisk(c *fiber.Ctx) error {
//		err := h.cluster.Write("./vectradb.snap")
//		if err != nil {
//			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
//		}
//		return c.JSON(fiber.Map{"status": "snapshot_saved", "path": "./vectradb.snap"})
//	}
func (h *Handler) CreateIndex(c *fiber.Ctx) error {
	go h.cluster.CreateIndex()
	return c.JSON(fiber.Map{"status": "index_creation_started"})
}
