package store

import (
	"encoding/json"
	"math/rand"
	"sync"
)

const (
	HNSW_M           = 16       // Max Neighbours per node
	MNSW_M0          = 32       // Max Neighbours at at layer 0 (usually 2+M)
	HNSW_EfConstruct = 100      // Candidates to check during ingestion
	HNSW_LevelMult   = 1 / 0.69 // Normalization factor for level generation
)

type HNSWNode struct {
	ID          string
	Vector      []float32
	Layer       int
	Connections [][]string //[Level][neighbourID]
	sync.RWMutex
}

type HNSWIndex struct {
	EntryNodeID string
	Nodes       map[string]*HNSWNode
	MaxLayer    int
	sync.RWMutex
}

// Return a new HNSW Index Tree
func NewHNSWIndex() *HNSWIndex {
	return &HNSWIndex{
		Nodes:    make(map[string]*HNSWNode),
		MaxLayer: -1,
	}
}

// randomLevel generates a level for a new node (Geometric Distribution)
func (h *HNSWIndex) randomLevel() int {
	lvl := 0
	for rand.Float64() < 0.5 {
		lvl++
	}
	return lvl
}

// Euclidean Distance (Squared) - Faster than Cosine for HNSW usually
// TODO: Use SIMD for pure math performance
func dist(v1, v2 []float32) float32 {
	var sum float32
	for i := range v1 {
		diff := v1[i] - v2[i]
		sum += diff * diff
	}
	return sum
}

// searchLayer finds the closest node to query in a specific layer
// starting from entry point
func (h *HNSWIndex) searchLayer(query []float32, entryPoint *HNSWNode, layer int) *HNSWNode {
	curr := entryPoint
	minDist := dist(query, curr.Vector)

	for {
		changed := false
		curr.RLock()
		friends := curr.Connections[layer]
		curr.RUnlock()

		for _, friendID := range friends {
			friendNode := h.Nodes[friendID]
			d := dist(query, friendNode.Vector)
			if d < minDist {
				minDist = d
				curr = friendNode
				changed = true
			}
		}

		// If we didn't find anyone closer we are at the local minimum
		if !changed {
			break
		}
	}
	return curr
}

func (h *HNSWIndex) Add(id string, vector []float32) {
	h.Lock()
	defer h.Unlock()

	level := h.randomLevel()
	newNode := &HNSWNode{
		ID:          id,
		Vector:      vector,
		Layer:       level,
		Connections: make([][]string, level+1),
	}
	h.Nodes[id] = newNode

	if h.EntryNodeID == "" {
		h.EntryNodeID = id
		h.MaxLayer = level
		return
	}

	curr := h.Nodes[h.EntryNodeID]

	for l := h.MaxLayer; l > level; l-- {
		curr = h.searchLayer(vector, curr, l)
	}

	for l := level; l >= 0; l-- {
		bestNeighbor := h.searchLayer(vector, curr, l)
		newNode.Connections[l] = append(newNode.Connections[l], bestNeighbor.ID)
		bestNeighbor.Connections[l] = append(bestNeighbor.Connections[l], id)

		curr = bestNeighbor
	}

	if level > h.MaxLayer {
		h.MaxLayer = level
		h.EntryNodeID = id
	}

}

func (h *HNSWIndex) Search(query []float32, k int) []VectroRecord {
	h.RLock()
	entryID := h.EntryNodeID
	maxL := h.MaxLayer
	h.RUnlock()

	if entryID == "" {
		return nil
	}

	curr := h.Nodes[entryID]

	for l := maxL; l > 0; l-- {
		curr = h.searchLayer(query, curr, l)
	}

	bestNode := h.searchLayer(query, curr, 0)

	return []VectroRecord{
		{
			ID:    bestNode.ID,
			Score: 1 - dist(query, bestNode.Vector),
			Data:  json.RawMessage{},
		},
	}
}
