package store

import (
	"hash/fnv"
	"sort"
	"sync"
)

type ShardHandler interface {
	Insert(id string, vector []float32, data interface{}) error
	Search(query []float32, topK int) []VectroRecord
	Delete(id string) error
}

type Cluster struct {
	shards    []ShardHandler
	numShards int
}

func NewCluster(shards []ShardHandler) *Cluster {
	return &Cluster{
		shards:    shards,
		numShards: len(shards),
	}
}

func (c *Cluster) GetShard(id string) ShardHandler {
	h := fnv.New32a()
	h.Write([]byte(id))
	idx := int(h.Sum32()) % c.numShards
	if idx < 0 {
		idx = -idx
	}
	return c.shards[idx]
}

func (c *Cluster) GetShardByID(n int) ShardHandler {
	if n < 0 || n >= c.numShards {
		return nil
	}
	return c.shards[n]
}

func (c *Cluster) Insert(id string, vector []float32, data any) error {
	targetShard := c.GetShard(id)
	return targetShard.Insert(id, vector, data)
}

func (c *Cluster) Search(query []float32, topK int) []VectroRecord {
	var wg sync.WaitGroup

	resultCh := make(chan []VectroRecord, c.numShards)

	for _, shard := range c.shards {
		wg.Add(1)
		go func(s ShardHandler) {
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

func (c *Cluster) Delete(id string) error {
	targetShard := c.GetShard(id)
	return targetShard.Delete(id)
}
