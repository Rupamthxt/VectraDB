package store

import (
	"fmt"
	"sync"
	"unsafe"
)

const PageSizeBytes = 4 * 1024 * 1024 // 4MB

type VectorArena struct {
	mu sync.RWMutex

	dim            int
	bytesPerVector int
	pages          [][]byte

	// Metadata to trace position
	currentPageIdx int
	currentVecIdx  int
	vectorsPerPage int
	totalVectors   uint32
}

// Initializes arena with a pre allocated capacity
func NewVectorArena(dim int) *VectorArena {

	// vecSizeBytes := dim * 4 // 4bytes per float32

	bytesPerVec := dim + 8 // 1 byter for each vector + 4 bytes Min + 4 bytes Max
	count := PageSizeBytes / bytesPerVec

	return &VectorArena{

		dim:            dim,
		pages:          make([][]byte, 0),
		currentPageIdx: 0,
		currentVecIdx:  0,
		totalVectors:   0,
		vectorsPerPage: count,
		bytesPerVector: bytesPerVec,
	}
}

// Inserts a vector in the arena and returns its global index
func (a *VectorArena) Add(vector []float32) (uint32, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(vector) != a.dim {
		return 0, fmt.Errorf("vector dimension mismatch expected %d got %d", a.dim, len(vector))
	}

	qv := Quantize(vector)

	if a.currentVecIdx >= a.vectorsPerPage || len(a.pages) == 0 {
		// Allocate a new page
		newPage := make([]byte, a.bytesPerVector*a.vectorsPerPage)
		a.pages = append(a.pages, newPage)

		if len(a.pages) > 1 {
			a.currentPageIdx++
		}
		a.currentVecIdx = 0
	}

	// Calculate offset in bytes
	// (index * dim * 4bytes)
	offset := a.currentVecIdx * a.bytesPerVector

	// Ensure we are writing to the LATEST page
	// Using len(a.pages)-1 is safer than trusting currentPageIdx if sync gets weird
	targetPage := a.pages[len(a.pages)-1]
	destination := targetPage[offset : offset+a.bytesPerVector]

	// Unsafe Copy
	// Note: This relies on architecture being Little Endian (Standard on x86/ARM)
	srcPtr := unsafe.Pointer(&qv.Data[0])
	srcBytes := unsafe.Slice((*byte)(srcPtr), len(qv.Data)) // int8 slice to byte slice
	copy(destination, srcBytes)

	// Write Min and Max at the end of the vector bytes
	minPtr := unsafe.Pointer(&qv.Min)
	srcBytes = unsafe.Slice((*byte)(minPtr), 4) // float32 is 4 bytes
	copy(targetPage[offset+a.dim:], srcBytes)
	maxPtr := unsafe.Pointer(&qv.Max)
	srcBytes = unsafe.Slice((*byte)(maxPtr), 4) // float32 is 4 bytes
	copy(targetPage[offset+a.dim+4:], srcBytes)

	// Calculate Global ID
	// Logic: (Completed Pages * Size) + Current Index
	globalId := uint32((len(a.pages)-1)*a.vectorsPerPage + a.currentVecIdx)

	a.currentVecIdx++
	a.totalVectors++

	return globalId, nil
}

// Retrieves a vector by its global index
func (a *VectorArena) Get(index uint32) (QuantizedVector, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Calculate page and offset
	if index >= a.totalVectors {
		return QuantizedVector{}, fmt.Errorf("Index out of bounds")
	}

	pageIdx := int(index) / a.vectorsPerPage
	vecIdxInPage := int(index) % a.vectorsPerPage

	// Calculate byte offset within the page
	offset := vecIdxInPage * a.bytesPerVector
	rawbytes := a.pages[pageIdx][offset : offset+a.dim]

	// Convert bytes to []float32 (Zero copy view)
	// For safety making a copy now

	out := make([]int8, a.dim)

	ptr := unsafe.Pointer(&rawbytes[0])
	srcFloats := unsafe.Slice((*int8)(ptr), a.dim)
	outMax := make([]float32, 1)
	outMin := make([]float32, 1)

	rawbytes = a.pages[pageIdx][offset+a.dim : offset+a.dim+4]
	ptr = unsafe.Pointer(&rawbytes[0])
	srcMin := unsafe.Slice((*float32)(ptr), 4)
	copy(outMin, srcMin)

	rawbytes = a.pages[pageIdx][offset+a.dim+4 : offset+a.dim+8]
	ptr = unsafe.Pointer(&rawbytes[0])
	srcMax := unsafe.Slice((*float32)(ptr), 4)
	copy(outMax, srcMax)

	// Convert bytes back to float32 slice
	copy(out, srcFloats)

	qv := QuantizedVector{
		Data: out,
		Min:  outMin[0],
		Max:  outMax[0],
	}

	return qv, nil
}

// Returns the total number of vectors stored
func (a *VectorArena) Size() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int(a.totalVectors)
}
