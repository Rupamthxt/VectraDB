package store

import (
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type Cluster struct {
	shards    []*VectraDB
	numShards int
}

func NewCluster(numShards, dim int, basePath string) (*Cluster, error) {
	shards := make([]*VectraDB, numShards)

	for i := range numShards {
		shardPath := filepath.Join(basePath, fmt.Sprintf("shard_%d", i))
		if err := os.MkdirAll(shardPath, 0755); err != nil {
			return nil, err
		}

		metaPath := filepath.Join(shardPath, "meta.bin")

		db, err := NewVectraDB(dim, metaPath)
		if err != nil {
			return nil, fmt.Errorf("failed to init shard %d: %w", i, err)
		}
		shards[i] = db
	}

	return &Cluster{
		shards:    shards,
		numShards: numShards,
	}, nil
}

// Returns a specific shard based on the hash of the ID
func (c *Cluster) getShard(id string) *VectraDB {
	h := fnv.New32a()
	h.Write([]byte(id))
	idx := int(h.Sum32()) % c.numShards
	if idx < 0 {
		idx = -idx
	}
	return c.shards[idx]
}

// Inserts data into the appropriate shard
func (c *Cluster) Insert(id string, vector []float32, data any) error {
	targetShard := c.getShard(id)
	return targetShard.Insert(id, vector, data)
}

// Searches across all shards and aggregates results
func (c *Cluster) Search(query []float32, topK int) []VectroRecord {
	var wg sync.WaitGroup

	resultCh := make(chan []VectroRecord, c.numShards)

	for _, shard := range c.shards {
		wg.Add(1)
		go func(s *VectraDB) {
			defer wg.Done()
			resultCh <- s.Search(query, topK)
		}(shard)
	}

	wg.Wait()
	close(resultCh)

	allMatches := make([]VectroRecord, 0, topK*c.numShards)
	for shardResults := range resultCh {
		allMatches = append(allMatches, shardResults...)
	}

	sort.Slice(allMatches, func(i, j int) bool {
		return allMatches[i].Score > allMatches[j].Score
	})

	if len(allMatches) > topK {
		return allMatches[:topK]
	}
	return allMatches
}

// Creates index on all shards concurrently
func (c *Cluster) CreateIndex() {
	var wg sync.WaitGroup
	for _, shard := range c.shards {
		wg.Add(1)
		go func(s *VectraDB) {
			defer wg.Done()
			s.AutoTuneIndex()
			s.CreateIndex()
		}(shard)
	}
	wg.Wait()
}
