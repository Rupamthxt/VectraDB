package store

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
)

type VectroRecord struct {
	ID    string
	Score float32
	Data  json.RawMessage
}

type VectraDB struct {
	mu       sync.RWMutex
	index    map[string]uint32
	revIndex []string

	// Hot Path Storage
	arena *VectorArena

	// Cold Path Storage
	metadata map[uint32][]byte

	dim int

	ivf *IVFIndex
}

func NewVectraDB(dim int) *VectraDB {
	return &VectraDB{
		index:    make(map[string]uint32),
		revIndex: make([]string, 10000),
		arena:    NewVectorArena(dim, 10000),
		metadata: make(map[uint32][]byte),
		dim:      dim,
		ivf:      NewIVFIndex(100),
	}
}

func (db *VectraDB) Insert(id string, vector []float32, data any) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	bytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("Failed to marshal metadata: %w", err)
	}

	idx, err := db.arena.Add(vector)
	if err != nil {
		return err
	}
	db.index[id] = idx
	db.revIndex = append(db.revIndex, id)

	db.metadata[idx] = bytes

	return nil
}

func (db *VectraDB) Get(id string) ([]float32, []byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	idx, exists := db.index[id]
	if !exists {
		return nil, nil, false
	}

	vec, _ := db.arena.Get(idx)
	meta := db.metadata[idx]
	return vec, meta, true
}

func (db *VectraDB) Search(query []float32, topK int) []VectroRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.ivf.IsTrained {
		return db.searchIVF(query, topK)
	}

	return db.searchBruteForce(query, topK)
}

func (db *VectraDB) searchBruteForce(query []float32, topK int) []VectroRecord {
	heap := make(MinHeap, 0, topK)

	arenaData := db.arena.data
	count := int(db.arena.nextIndex)
	dim := db.dim

	for i := range count {
		start := i * dim
		end := start + dim

		targetVec := arenaData[start:end]
		score := cosineSimilarity(query, targetVec)

		if len(heap) < topK {
			heap.Push(Match{Index: uint32(i), Score: score})
		} else if score > heap[0].Score {
			heap.Replace(Match{Index: uint32(i), Score: score})
		}
	}

	return db.finalizeResults(heap)
}

func (db *VectraDB) searchIVF(query []float32, topK int) []VectroRecord {

	bestCluster := 0
	bestScore := float32(-1.0)

	for i, centroid := range db.ivf.Centroids {
		score := cosineSimilarity(query, centroid)
		if score > bestScore {
			bestScore = score
			bestCluster = i
		}
	}

	candidatesIdx := db.ivf.Buckets[bestCluster]

	heap := make(MinHeap, 0, topK)

	for _, idx := range candidatesIdx {
		vec, _ := db.arena.Get(idx)
		score := cosineSimilarity(query, vec)

		if len(heap) < topK {
			heap.Push(Match{Index: idx, Score: score})
		} else if score > heap[0].Score {
			heap.Replace(Match{Index: idx, Score: score})
		}
	}

	return db.finalizeResults(heap)
}

func (db *VectraDB) finalizeResults(heap MinHeap) []VectroRecord {
	sort.Slice(heap, func(i, j int) bool {
		return heap[i].Score > heap[j].Score
	})

	results := make([]VectroRecord, 0, len(heap))

	for _, match := range heap {
		internalIdx := match.Index

		if internalIdx >= uint32(len(db.revIndex)) {
			continue
		}

		realID := db.revIndex[internalIdx]
		meta := db.metadata[internalIdx]

		results = append(results, VectroRecord{
			ID:    realID,
			Score: match.Score,
			Data:  meta,
		})
	}

	return results
}

func cosineSimilarity(a, b []float32) float32 {
	var dot, mag1, mag2 float32
	for i := range a {
		dot += a[i] * b[i]
		mag1 += a[i] * a[i]
		mag2 += b[i] * b[i]
	}
	if mag1 == 0 || mag2 == 0 {
		return 0
	}
	return dot / (float32(math.Sqrt(float64(mag1)))) * float32(math.Sqrt(float64(mag2)))

}

func (db *VectraDB) CreateIndex() {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Train with 10 iterations
	db.ivf.Train(db.arena, 10)
}
