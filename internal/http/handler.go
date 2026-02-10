package http

import (
	"encoding/json"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rupamthxt/vectradb/internal/metrics"
	"github.com/rupamthxt/vectradb/internal/store"
)

type Handler struct {
	cluster *store.Cluster
}

func NewHandler(cluster *store.Cluster) *Handler {
	return &Handler{cluster: cluster}
}

func (h *Handler) Insert(c *fiber.Ctx) error {
	// Start Timer
	start := time.Now()
	metrics.InsertRequests.Inc() // +1 to Counter

	var req InsertRequest

	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot parse json"})
	}

	if req.ID == "" || len(req.Vector) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "id and vector are required"})
	}

	err := h.cluster.Insert(req.ID, req.Vector, req.Data)

	// Record Duration
	duration := time.Since(start).Seconds()
	metrics.InsertDuration.Observe(duration)

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	// Update Vector Count
	metrics.TotalVectors.Inc()
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"message": "data inserted successfully"})
}

func (h *Handler) Search(c *fiber.Ctx) error {
	start := time.Now()
	metrics.SearchRequests.Inc()
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

	duration := time.Since(start).Seconds()
	metrics.SearchDuration.Observe(duration)

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

func (h *Handler) CreateIndex(c *fiber.Ctx) error {
	go h.cluster.CreateIndex()
	return c.JSON(fiber.Map{"status": "index_creation_started"})
}
