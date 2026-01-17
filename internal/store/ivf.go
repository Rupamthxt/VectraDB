package store

import (
	"math/rand"
)

type IVFIndex struct {
	Centroids   [][]float32
	Buckets     map[int][]uint32
	IsTrained   bool
	NumClusters int
}

func NewIVFIndex(numClusters int) *IVFIndex {
	return &IVFIndex{
		Centroids:   make([][]float32, 0),
		Buckets:     make(map[int][]uint32),
		IsTrained:   false,
		NumClusters: numClusters,
	}
}

func (ivf *IVFIndex) Train(arena *VectorArena, iter int) {
	dataSize := int(arena.nextIndex)
	if dataSize < ivf.NumClusters {
		//Not enough data to form clusters
		return
	}

	dim := arena.dim

	ivf.Centroids = make([][]float32, ivf.NumClusters)

	for i := range ivf.NumClusters {
		randIdx := uint32(rand.Intn(dataSize))
		vec, _ := arena.Get(randIdx)

		centroid := make([]float32, dim)
		copy(centroid, vec)
		ivf.Centroids[i] = centroid
	}

	for n := range iter {

		//Temporary buckets for iteration
		newBuckets := make(map[int][]uint32)
		clusterSums := make([][]float32, ivf.NumClusters)
		clusterCounts := make([]int, ivf.NumClusters)

		//Init Sums
		for k := 0; k < ivf.NumClusters; k++ {
			clusterSums[k] = make([]float32, dim)
		}

		for i := 0; i < dataSize; i++ {
			vec, _ := arena.Get(uint32(i))

			bestCluster := 0
			bestScore := float32(-1.0)

			for c := 0; c < ivf.NumClusters; c++ {
				score := cosineSimilarity(vec, ivf.Centroids[c])
				if score > bestScore {
					bestScore = score
					bestCluster = c
				}
			}

			newBuckets[bestCluster] = append(newBuckets[bestCluster], uint32(i))
			addToVector(clusterSums[bestCluster], vec)
			clusterCounts[bestCluster]++
		}

		//Update centroids
		for k := 0; k < ivf.NumClusters; k++ {
			if clusterCounts[k] > 0 {
				divVector(clusterSums[k], float32(clusterCounts[k]))
				ivf.Centroids[k] = clusterSums[k]
			}
		}

		if n == iter-1 {
			ivf.Buckets = newBuckets
		}
	}
	ivf.IsTrained = true
}
