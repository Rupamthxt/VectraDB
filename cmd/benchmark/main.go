package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/rupamthxt/vectradb/internal/store"
)

const (
	Dimension    = 128
	TotalVectors = 100_000
	TotalQueries = 1000
	K            = 10
	NumWorkers   = 8 // Concurrency
)

func main() {
	fmt.Printf("🔥 Starting VectraDB Benchmark\n")
	fmt.Printf("Config: Dim=%d | Items=%d | Queries=%d\n\n", Dimension, TotalVectors, TotalQueries)

	db := store.NewVectraDB(Dimension)

	// --- PHASE 1: INGESTION ---
	fmt.Println("--- Phase 1: Ingestion ---")
	vectors := make([][]float32, TotalVectors)
	ids := make([]string, TotalVectors)
	for i := 0; i < TotalVectors; i++ {
		vectors[i] = randomVector(Dimension)
		ids[i] = fmt.Sprintf("item_%d", i)
	}

	startWrite := time.Now()
	for i := 0; i < TotalVectors; i++ {
		_ = db.Insert(ids[i], vectors[i], nil)
	}
	fmt.Printf("✅ Inserted %d vectors in %v\n", TotalVectors, time.Since(startWrite))

	// Pre-generate queries
	queries := make([][]float32, TotalQueries)
	for i := 0; i < TotalQueries; i++ {
		queries[i] = randomVector(Dimension)
	}

	// --- PHASE 2: BRUTE FORCE SEARCH ---
	fmt.Println("\n--- Phase 2: Brute Force Search ---")
	startRead := time.Now()
	runConcurrentSearch(db, queries)
	durationRead := time.Since(startRead)
	fmt.Printf("🚀 Brute Force QPS: %.2f\n", float64(TotalQueries)/durationRead.Seconds())

	// --- PHASE 3: TRAINING IVF ---
	fmt.Println("\n--- Phase 3: Training IVF Index ---")
	startTrain := time.Now()
	db.CreateIndex() // <--- This triggers the K-Means Clustering
	fmt.Printf("✅ Index Trained in %v\n", time.Since(startTrain))

	// --- PHASE 4: IVF SEARCH ---
	fmt.Println("\n--- Phase 4: IVF (Approximate) Search ---")
	startIVF := time.Now()
	runConcurrentSearch(db, queries)
	durationIVF := time.Since(startIVF)
	fmt.Printf("🚀 IVF QPS: %.2f\n", float64(TotalQueries)/durationIVF.Seconds())

	speedup := (float64(TotalQueries) / durationIVF.Seconds()) / (float64(TotalQueries) / durationRead.Seconds())
	fmt.Printf("\n⚡ Speedup Factor: %.2fx\n", speedup)
}

func runConcurrentSearch(db *store.VectraDB, queries [][]float32) {
	var wg sync.WaitGroup
	chunkSize := len(queries) / NumWorkers

	for w := 0; w < NumWorkers; w++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < chunkSize; i++ {
				_ = db.Search(queries[offset+i], K)
			}
		}(w * chunkSize)
	}
	wg.Wait()
}

func randomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = rand.Float32()
	}
	return vec
}
