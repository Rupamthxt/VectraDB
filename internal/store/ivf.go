package store

import (
	"fmt"
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
	step := dataSize / ivf.NumClusters // Calculate step size

	for i := 0; i < ivf.NumClusters; i++ {
		// Pick indices evenly: 0, 50000, 100000...
		idx := uint32(i * step)
		if int(idx) >= dataSize {
			idx = uint32(dataSize - 1)
		} // Safety

		vec, _ := arena.Get(idx)

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

	// --- DEBUG: Print Bucket Stats ---
	minSize := 999999999
	maxSize := 0
	nonEmpty := 0
	total := 0

	for _, ids := range ivf.Buckets {
		size := len(ids)
		if size > 0 {
			nonEmpty++
			if size < minSize {
				minSize = size
			}
			if size > maxSize {
				maxSize = size
			}
			total += size
		}
	}

	fmt.Printf("\n--- IVF Debug Stats ---\n")
	fmt.Printf("Total Clusters: %d\n", ivf.NumClusters)
	fmt.Printf("Non-Empty Clusters: %d\n", nonEmpty)
	fmt.Printf("Min Bucket Size: %d\n", minSize)
	fmt.Printf("Max Bucket Size: %d\n", maxSize)
	fmt.Printf("Avg Bucket Size: %d\n", total/ivf.NumClusters)
	fmt.Printf("-----------------------\n")
}
