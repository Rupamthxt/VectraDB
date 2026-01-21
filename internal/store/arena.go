package store

import (
	"fmt"
	"sync"
)

const VectorsPerPage = 50000

type VectorArena struct {
	mu sync.RWMutex
	// data      []float32 // Slab of Memory
	// dim       int       // Dimension of Vectors
	// nextIndex uint32    //   Next available slot index

	dim            int
	pages          [][]float32
	currentPageIdx int
	currentVecIdx  int

	totalVectors uint32
}

// Initializes arena with a pre allocated capacity
func NewVectorArena(dim int) *VectorArena {

	firstPage := make([]float32, dim*VectorsPerPage)

	return &VectorArena{
		// Pre-allocate memory to avoid resizing
		// data:      make([]float32, 0, capacity*dim),
		// dim:       dim,
		// nextIndex: 0,

		dim:            dim,
		pages:          [][]float32{firstPage},
		currentPageIdx: 0,
		currentVecIdx:  0,
		totalVectors:   0,
	}
}

func (a *VectorArena) Add(vector []float32) (uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(vector) != a.dim {
		return 0, fmt.Errorf("vector dimension mismatch expected %d got %d", a.dim, len(vector))
	}

	if a.currentPageIdx >= VectorsPerPage {
		// Allocate a new page
		newPage := make([]float32, a.dim*VectorsPerPage)
		a.pages = append(a.pages, newPage)

		a.currentPageIdx++
		a.currentVecIdx = 0
	}

	offset := a.currentPageIdx * a.dim
	copy(a.pages[a.currentPageIdx][offset:offset], vector)

	globalId := uint32((a.currentPageIdx * VectorsPerPage) + a.currentVecIdx)

	a.currentVecIdx++
	a.totalVectors++

	return globalId, nil
}

func (a *VectorArena) Get(index uint32) ([]float32, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if index >= a.totalVectors {
		return nil, fmt.Errorf("Index out of bounds")
	}

	pageIdx := int(index) / VectorsPerPage
	vecIdxInPage := int(index) % VectorsPerPage

	offset := vecIdxInPage * a.dim

	return a.pages[pageIdx][offset : offset+a.dim], nil
}

func (a *VectorArena) Size() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int(a.totalVectors)
}
