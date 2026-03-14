package store

import (
	"encoding/json"
	"math/rand"
	"sort"
	"sync"
)

const (
	HNSW_M           = 16       // Max Neighbours per node
	MNSW_M0          = 32       // Max Neighbours at layer 0 (usually 2*M)
	HNSW_EfConstruct = 100      // Candidates to check during ingestion
	HNSW_LevelMult   = 1 / 0.69 // Normalization factor for level generation
)

type HNSWNode struct {
	ID          string
	Layer       int
	Connections [][]string //[Level][neighbourID]
	ArenaOffset uint32
	sync.RWMutex
}

type HNSWIndex struct {
	EntryNodeID string
	Nodes       map[string]*HNSWNode
	MaxLayer    int
	Arena       *VectorArena
	sync.RWMutex
}

type nodeDist struct {
	node *HNSWNode
	dist float32
}

// Return a new HNSW Index Tree
func NewHNSWIndex(arena *VectorArena) *HNSWIndex {
	return &HNSWIndex{
		Nodes:    make(map[string]*HNSWNode),
		MaxLayer: -1,
		Arena:    arena,
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
func (h *HNSWIndex) searchLayer(query []float32, entryPoint *HNSWNode, layer int) (*HNSWNode, error) {
	curr := entryPoint
	currVector, err := h.Arena.Get(uint32(curr.ArenaOffset))
	if err != nil {
		return nil, err
	}
	minDist := dist(query, currVector)

	for {
		changed := false
		curr.RLock()
		friends := curr.Connections[layer]
		curr.RUnlock()

		for _, friendID := range friends {
			friendNode := h.Nodes[friendID]
			friendVector, err := h.Arena.Get(uint32(friendNode.ArenaOffset))
			if err != nil {
				return nil, err
			}
			d := dist(query, friendVector)
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
	return curr, nil
}

// Add's a new node to the HNSW graph, connecting it to existing nodes based on proximity
func (h *HNSWIndex) Add(vector []float32, id string, idx uint32) {
	h.Lock()
	defer h.Unlock()

	// If this vector ID already exists in the graph,
	// do not insert it again.
	if _, exists := h.Nodes[id]; exists {
		return
	}

	// Create New Node with random level
	level := h.randomLevel()
	newNode := &HNSWNode{
		ID:          id,
		Layer:       level,
		Connections: make([][]string, level+1),
		ArenaOffset: idx,
	}
	h.Nodes[id] = newNode

	// If graph is empty, set this as entry point
	if h.EntryNodeID == "" {
		h.EntryNodeID = id
		h.MaxLayer = level
		return
	}

	curr := h.Nodes[h.EntryNodeID]

	// Zoom Phase: Search down from top layer to the nodes level
	// We doon't link yet, just find the best starting point
	for l := h.MaxLayer; l > level; l-- {
		curr, _ = h.searchLayer(vector, curr, l)
	}

	startLayer := level
	if h.MaxLayer < level {
		startLayer = h.MaxLayer
	}

	// Build Phase: Link neighbours from node's level down to 0
	for l := startLayer; l >= 0; l-- {
		// Find the closest neighbor at this layer
		// TODO: In prod, we'd find 'M' nighbors, here we simplify to 1 for redability
		bestNeighbor, _ := h.searchLayer(vector, curr, l)

		// Link them (Bidirectional)
		newNode.Connections[l] = append(newNode.Connections[l], bestNeighbor.ID)
		bestNeighbor.Connections[l] = append(bestNeighbor.Connections[l], id)

		// Move search pointer for next iteration
		curr = bestNeighbor
	}

	// Update Entry Point if new node is higher
	if level > h.MaxLayer {
		h.MaxLayer = level
		h.EntryNodeID = id
	}

}

// Search finds and returns the k closest nodes to the query vector using the HNSW algorithm.
// For simplicity, we return only the closest node here,
// but in production, we'd maintain a priority queue of candidates to return top K results.
// func (h *HNSWIndex) Search(query []float32, k int) []VectroRecord {
// 	h.RLock()
// 	entryID := h.EntryNodeID
// 	maxL := h.MaxLayer
// 	h.RUnlock()

// 	if entryID == "" {
// 		return nil
// 	}

// 	curr := h.Nodes[entryID]

// 	for l := maxL; l > 0; l-- {
// 		curr, _ = h.searchLayer(query, curr, l)
// 	}

// 	bestNode, _ := h.searchLayer(query, curr, 0)
// 	bestVector, _ := h.Arena.Get(bestNode.ArenaOffset)

// 	return []VectroRecord{
// 		{
// 			ID:    bestNode.ID,
// 			Score: 1 - dist(query, bestVector),
// 			Data:  json.RawMessage{},
// 		},
// 	}
// }

func (h *HNSWIndex) Search(query []float32, k int) []VectroRecord {
	h.RLock()
	entryID := h.EntryNodeID
	maxL := h.MaxLayer
	h.RUnlock()

	if entryID == "" {
		return nil
	}

	curr := h.Nodes[entryID]

	// ZOOM PHASE: Fast traversal down to Layer 1 (Finds a great starting point)
	for l := maxL; l > 0; l-- {
		curr, _ = h.searchLayer(query, curr, l)
	}

	// BUILD THE NET: Layer 0 Top-K Search
	// ef (Exploration Factor) controls accuracy vs speed.
	// In production, ef is usually higher than K (e.g., K=10, ef=64).
	ef := k
	if ef < 10 {
		ef = 10
	}

	// Prevent infinite loops in the graph
	visited := make(map[string]bool)
	visited[curr.ID] = true

	currVec, _ := h.Arena.Get(curr.ArenaOffset)
	currDist := dist(query, currVec)

	// 'candidates' are nodes we still need to explore. 'results' are the best ones we've found.
	candidates := []nodeDist{{node: curr, dist: currDist}}
	results := []nodeDist{{node: curr, dist: currDist}}

	for len(candidates) > 0 {
		// Pop the closest node from candidates
		c := candidates[0]
		candidates = candidates[1:]

		// What is the worst result we currently consider "good"?
		furthestResultDist := results[len(results)-1].dist

		// EARLY STOPPING: If our closest candidate is further away than our worst result,
		// and we already have enough results, the search space is exhausted!
		if c.dist > furthestResultDist && len(results) >= k {
			break
		}

		c.node.RLock()
		// Get all friends at Layer 0
		friends := c.node.Connections[0]
		c.node.RUnlock()

		for _, friendID := range friends {
			if !visited[h.Nodes[friendID].ID] {
				visited[h.Nodes[friendID].ID] = true

				fVec, _ := h.Arena.Get(h.Nodes[friendID].ArenaOffset)
				fDist := dist(query, fVec)

				// If this friend is better than our worst result, or we don't have enough results yet
				if fDist < furthestResultDist || len(results) < k {
					candidates = append(candidates, nodeDist{node: h.Nodes[friendID], dist: fDist})
					results = append(results, nodeDist{node: h.Nodes[friendID], dist: fDist})

					// Sort candidates ascending (closest first)
					sort.Slice(candidates, func(i, j int) bool {
						return candidates[i].dist < candidates[j].dist
					})

					// Sort results ascending and trim to our exploration factor (ef)
					sort.Slice(results, func(i, j int) bool {
						return results[i].dist < results[j].dist
					})
					if len(results) > ef {
						results = results[:ef]
					}
				}
			}
		}
	}

	// FORMAT THE OUTPUT
	// Trim down to exactly K items if we gathered more
	if len(results) > k {
		results = results[:k]
	}

	var output []VectroRecord
	for _, r := range results {
		output = append(output, VectroRecord{
			ID:    r.node.ID,
			Score: 1 - r.dist, // Assuming your dist() is Euclidean/Cosine converted to a similarity score
			Data:  json.RawMessage(`{}`),
		})
	}

	return output
}
