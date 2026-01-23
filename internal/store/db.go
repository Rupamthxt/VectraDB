package store

import (
	"encoding/binary"
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
	// metadata map[uint32][]byte

	metaLocs map[uint32]FileLocation

	disk *DiskStore

	dim int

	ivf *IVFIndex
}

func NewVectraDB(dim int, storagePath string) (*VectraDB, error) {

	ds, err := NewDiskStore(storagePath)
	if err != nil {
		return nil, fmt.Errorf("Failed to init disk store at %s: %w", storagePath, err)
	}

	return &VectraDB{
		index:    make(map[string]uint32),
		revIndex: make([]string, 10000),
		arena:    NewVectorArena(dim),
		// metadata: make(map[uint32][]byte),
		metaLocs: make(map[uint32]FileLocation),
		disk:     ds,
		dim:      dim,
		ivf:      NewIVFIndex(2000),
	}, nil
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

	loc, err := db.disk.Write(bytes)
	if err != nil {
		return err
	}

	db.index[id] = idx
	db.revIndex = append(db.revIndex, id)

	db.metaLocs[idx] = loc

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
	metaLoc := db.metaLocs[idx]
	meta, err := db.disk.Read(metaLoc)
	if err != nil {
		return vec, nil, true
	}
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

	globalIndex := uint32(0)

	for _, page := range db.arena.pages {
		for i := 0; i < len(page); i += db.dim * 4 {
			if i+db.dim*4 > len(page) {
				break
			}

			vec := make([]float32, db.dim)
			for j := i; j < i+db.dim*4; j += 4 {
				vec[(j-i)/4] = math.Float32frombits(binary.LittleEndian.Uint32(page[j : j+4]))
			}
			if globalIndex >= db.arena.totalVectors {
				break
			}

			score := cosineSimilarity(query, vec)

			if len(heap) < topK {
				heap.Push(Match{Index: globalIndex, Score: score})
			} else if score > heap[0].Score {
				heap.Replace(Match{Index: globalIndex, Score: score})
			}

			globalIndex++
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

		loc, exists := db.metaLocs[internalIdx]
		var metadata []byte
		if exists {
			metadata, _ = db.disk.Read(loc)
		}

		results = append(results, VectroRecord{
			ID:    db.revIndex[internalIdx],
			Score: match.Score,
			Data:  metadata,
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

func (db *VectraDB) AutoTuneIndex() {
	db.mu.Lock()
	defer db.mu.Unlock()

	count := float64(db.arena.totalVectors)
	if count == 0 {
		return
	}

	// Calculate Sqrt(N)
	targetClusters := int(math.Sqrt(count))

	// Clamp values (don't go too small or too crazy big)
	if targetClusters < 10 {
		targetClusters = 10
	}
	if targetClusters > 5000 {
		targetClusters = 5000
	}

	fmt.Printf("Auto-Tuning: Recreating IVF with %d clusters for %d vectors\n", targetClusters, int(count))
	db.ivf = NewIVFIndex(targetClusters)
}

func (db *VectraDB) CreateIndex() {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Train with 10 iterations
	db.ivf.Train(db.arena, 10)
}
