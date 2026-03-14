package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileLocation holds the location of a vector in the disk
// in terms of offset and length
type FileLocation struct {
	Offset int64
	Length int32
}

// DiskStore is a simple disk-based storage system that allows
// appending data and reading it back using the FileLocation.
type DiskStore struct {
	mu   sync.RWMutex
	file *os.File
	pos  int64 // current write position
}

// NewDiskStore initializes a new DiskStore at the given path.
func NewDiskStore(path string) (*DiskStore, error) {

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directories for disk store: %w", err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("Failed to open disk store: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &DiskStore{
		file: f,
		pos:  info.Size(),
	}, nil
}

// Write appends data to the disk and returns its FileLocation
func (ds *DiskStore) Write(data []byte) (FileLocation, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	loc := FileLocation{
		Offset: ds.pos,
		Length: int32(len(data)),
	}

	n, err := ds.file.Write(data)
	if err != nil {
		return FileLocation{}, err
	}
	ds.pos += int64(n)
	return loc, nil
}

// Read retrieves data from the disk based on the provided FileLocation
func (ds *DiskStore) Read(loc FileLocation) ([]byte, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	buffer := make([]byte, loc.Length)

	_, err := ds.file.ReadAt(buffer, loc.Offset)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

// Close closes the underlying file of the DiskStore
func (ds *DiskStore) Close() error {
	return ds.file.Close()
}
