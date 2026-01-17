package store

import (
	"fmt"
	"sync"
)

type VectorArena struct {
	mu        sync.Mutex
	data      []float32 // Slab of Memory
	dim       int       // Dimension of Vectors
	nextIndex uint32    // Next available slot index
}

// Initializes arena with a pre allocated capacity
func NewVectorArena(dim int, capacity int) *VectorArena {
	return &VectorArena{
		// Pre-allocate memory to avoid resizing
		data:      make([]float32, 0, capacity*dim),
		dim:       dim,
		nextIndex: 0,
	}
}

func (a *VectorArena) Add(vector []float32) (uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(vector) != a.dim {
		return 0, fmt.Errorf("vector dimension mismatch expected %d got %d", a.dim, len(vector))
	}

	a.data = append(a.data, vector...)
	idx := a.nextIndex
	a.nextIndex++

	return idx, nil
}

func (a *VectorArena) Get(index uint32) ([]float32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if index >= a.nextIndex {
		return nil, fmt.Errorf("Index out of bounds")
	}

	start := int(index) * a.dim
	end := start + a.dim

	return a.data[start:end], nil
}

func (a *VectorArena) Size() uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.nextIndex
}
