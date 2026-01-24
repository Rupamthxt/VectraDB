package store

import (
	"fmt"
	"sync"
)

const PageSizeBytes = 4 * 1024 * 1024 // 4MB

type VectorArena struct {
	mu sync.RWMutex

	dim            int
	pages          [][]float32
	currentPageIdx int
	currentVecIdx  int
	vectorsPerPage int
	totalVectors   uint32
}

// Initializes arena with a pre allocated capacity
func NewVectorArena(dim int) *VectorArena {

	firstPage := make([]float32, PageSizeBytes/4) // 4MB page
	vecSizeBytes := dim * 4                       // 4bytes per float32
	count := PageSizeBytes / vecSizeBytes

	return &VectorArena{

		dim:            dim,
		pages:          [][]float32{firstPage},
		currentPageIdx: 0,
		currentVecIdx:  0,
		totalVectors:   0,
		vectorsPerPage: count,
	}
}

// Inserts a vector in the arena and returns its global index
func (a *VectorArena) Add(vector []float32) (uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(vector) != a.dim {
		return 0, fmt.Errorf("vector dimension mismatch expected %d got %d", a.dim, len(vector))
	}

	if a.currentVecIdx >= a.vectorsPerPage {
		// Allocate a new page
		newPage := make([]float32, PageSizeBytes/4)
		a.pages = append(a.pages, newPage)

		a.currentPageIdx++
		a.currentVecIdx = 0
	}

	// 1. Calculate offset using the VECTOR index
	start := a.currentVecIdx * a.dim
	end := start + a.dim

	// 2. Ensure we are writing to the LATEST page
	// Using len(a.pages)-1 is safer than trusting currentPageIdx if sync gets weird
	targetPage := a.pages[len(a.pages)-1]

	// 3. Copy data
	copy(targetPage[start:end], vector)

	// 4. Calculate Global ID
	// Logic: (Completed Pages * Size) + Current Index
	globalId := uint32((len(a.pages)-1)*a.vectorsPerPage + a.currentVecIdx)

	a.currentVecIdx++
	a.totalVectors++

	return globalId, nil
}

// Retrieves a vector by its global index
func (a *VectorArena) Get(index uint32) ([]float32, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if index >= a.totalVectors {
		return nil, fmt.Errorf("Index out of bounds")
	}

	pageIdx := int(index) / a.vectorsPerPage
	vecIdxInPage := int(index) % a.vectorsPerPage

	offset := vecIdxInPage * a.dim

	// Convert bytes back to float32 slice
	vec := a.pages[pageIdx][offset : offset+a.dim]

	return vec, nil
}

// Returns the total number of vectors stored
func (a *VectorArena) Size() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int(a.totalVectors)
}
