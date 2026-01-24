package store

import (
	"fmt"
	"sync"
	"unsafe"
)

const PageSizeBytes = 4 * 1024 * 1024 // 4MB

type VectorArena struct {
	mu sync.RWMutex

	dim   int
	pages [][]byte

	// Metadata to trace position
	currentPageIdx int
	currentVecIdx  int
	vectorsPerPage int
	totalVectors   uint32
}

// Initializes arena with a pre allocated capacity
func NewVectorArena(dim int) *VectorArena {

	vecSizeBytes := dim * 4 // 4bytes per float32
	count := PageSizeBytes / vecSizeBytes

	return &VectorArena{

		dim:            dim,
		pages:          make([][]byte, 0),
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

	if a.currentVecIdx >= a.vectorsPerPage || len(a.pages) == 0 {
		// Allocate a new page
		newPage := make([]byte, a.dim*4*a.vectorsPerPage) // dim * 4bytes * vectorsPerPage
		a.pages = append(a.pages, newPage)

		if len(a.pages) > 1 {
			a.currentPageIdx++
		}
		a.currentVecIdx = 0
	}

	// 1. Calculate offset in bytes
	// (index * dim * 4bytes)
	offset := a.currentVecIdx * a.dim * 4

	// 2. Ensure we are writing to the LATEST page
	// Using len(a.pages)-1 is safer than trusting currentPageIdx if sync gets weird
	targetPage := a.pages[len(a.pages)-1]
	destination := targetPage[offset : offset+(a.dim*4)]

	// 3. Unsafe Copy
	// Note: This relies on architecture being Little Endian (Standard on x86/ARM)
	srcPtr := unsafe.Pointer(&vector[0])
	srcBytes := unsafe.Slice((*byte)(srcPtr), len(vector)*4)

	copy(destination, srcBytes)

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

	// 1. Calculate page and offset
	if index >= a.totalVectors {
		return nil, fmt.Errorf("Index out of bounds")
	}

	pageIdx := int(index) / a.vectorsPerPage
	vecIdxInPage := int(index) % a.vectorsPerPage

	// 2. Calculate byte offset within the page
	offset := vecIdxInPage * a.dim * 4
	rawbytes := a.pages[pageIdx][offset : offset+(a.dim*4)]

	// 3. Convert bytes to []float32 (Zero copy view)
	// For safety making a copy now

	out := make([]float32, a.dim)

	ptr := unsafe.Pointer(&rawbytes[0])
	srcFloats := unsafe.Slice((*float32)(ptr), a.dim)

	// Convert bytes back to float32 slice
	copy(out, srcFloats)

	return out, nil
}

// Returns the total number of vectors stored
func (a *VectorArena) Size() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int(a.totalVectors)
}
