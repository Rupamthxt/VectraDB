package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	OpInsert = 1
	OpDelete = 2
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
	mutex  sync.RWMutex
}

// OpenWal creates a new Write Ahead Log and returns a WAL struct
func OpenWal(path string) (*WAL, error) {
	fmt.Printf("Opening WAL at path: %s\n", path)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file:   f,
		writer: bufio.NewWriter(f),
	}, nil
}

// WriterEntry saves an operation in the WAL

func (wal *WAL) WriteEntry(op byte, id string, vector []float32, metadata []byte, loc FileLocation) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	// Calculate sizes
	idBytes := []byte(id)
	idLen := uint32(len(idBytes))
	vectorLen := uint32(len(vector) * 4) // 4 bytes per float32
	metadataLen := uint32(len(metadata))

	// Total Payload Size: Op(1) + IDLen(4) + ID + VecLen(4) + Vec + MetaLen(4) + Meta
	totalPayloadSize := 1 + 4 + idLen + 4 + vectorLen + 4 + metadataLen + 8 + 4

	// Write Size Header
	if err := binary.Write(wal.writer, binary.LittleEndian, uint32(totalPayloadSize)); err != nil {
		return err
	}

	// Write operation type
	if err := wal.writer.WriteByte(op); err != nil {
		return err
	}

	// Write ID
	binary.Write(wal.writer, binary.LittleEndian, idLen)
	wal.writer.Write(idBytes)

	// Write Vector (Convert float32 -> bytes)
	binary.Write(wal.writer, binary.LittleEndian, vectorLen)
	for _, v := range vector {
		// Optimization: In prod, use unsafe.Slice or math.Float32bits to bulk write
		binary.Write(wal.writer, binary.LittleEndian, v)
	}

	// Write Metadata
	binary.Write(wal.writer, binary.LittleEndian, metadataLen)
	wal.writer.Write(metadata)

	// Write Offset (8 bytes)
	if err := binary.Write(wal.writer, binary.LittleEndian, int64(loc.Offset)); err != nil {
		return err
	}
	// Write Len (4 bytes)
	if err := binary.Write(wal.writer, binary.LittleEndian, uint32(loc.Length)); err != nil {
		return err
	}

	if err := wal.writer.Flush(); err != nil {
		return err
	}
	return nil
}

// Close ensures everything is written to disk
func (w *WAL) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

// Iterator function type
type WALIterator func(id string, vector []float32, meta []byte, loc FileLocation)

func (wal *WAL) Recover(fn WALIterator) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()
	// Reset file pointer to start
	wal.file.Seek(0, 0)
	reader := bufio.NewReader(wal.file)

	for {
		// 1. Read Payload Size
		var size uint32
		if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
			if err == io.EOF {
				break // End of file, we are done
			}
			return err
		}

		// 2. Read Operation
		op, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break // End of file, we are done
			}
			return err
		}

		// 3. Read ID
		var idLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &idLen); err != nil {
			return err
		}
		idBytes := make([]byte, idLen)
		if _, err := reader.Read(idBytes); err != nil {
			return err
		}
		id := string(idBytes)

		// 4. Read Vector
		var vecLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &vecLen); err != nil {
			return err
		}
		vecCount := vecLen / 4
		vector := make([]float32, vecCount)
		for i := 0; i < int(vecCount); i++ {
			if err := binary.Read(reader, binary.LittleEndian, &vector[i]); err != nil {
				return err
			}
		}

		// 5. Read Metadata
		var metaLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &metaLen); err != nil {
			return err
		}
		meta := make([]byte, metaLen)
		if _, err := reader.Read(meta); err != nil {
			return err
		}

		// Read Offset
		var offset int64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return err
		}

		// Read Length after offset
		var locLen int32
		if err := binary.Read(reader, binary.LittleEndian, &locLen); err != nil {
			return err
		}

		// 6. Execute callback (Re-insert into DB)
		if op == OpInsert {
			fn(id, vector, meta, FileLocation{Offset: offset, Length: locLen})
		}
	}

	// Move pointer back to end for appending new writes
	wal.file.Seek(0, 2)
	return nil
}

func (wal *WAL) Sync() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()
	return wal.file.Sync()
}
