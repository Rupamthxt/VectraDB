package store

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

const (
	OpInsert = 1
	OpDelete = 2
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
}

// OpenWal creates a new Write Ahead Log and returns a WAL struct
func OpenWal(path string) (*WAL, error) {
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

func (wal *WAL) WriteEntry(op byte, id string, vector []float32, metadata []byte) error {
	// Calculate sizes
	idBytes := []byte(id)
	idLen := uint32(len(idBytes))
	vectorLen := uint32(len(vector) * 4) // 4 bytes per float32
	metadataLen := uint32(len(metadata))

	// Total Payload Size: Op(1) + IDLen(4) + ID + VecLen(4) + Vec + MetaLen(4) + Meta
	totalPayloadSize := 1 + 4 + idLen + 4 + vectorLen + 4 + metadataLen

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

	return wal.writer.Flush()
}

// Close ensures everything is written to disk
func (w *WAL) Close() error {
	w.writer.Flush()
	return w.file.Close()
}

// Iterator function type
type WALIterator func(id string, vector []float32, meta []byte)

func (wal *WAL) Recover(fn WALIterator) error {
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
		op, _ := reader.ReadByte()

		// 3. Read ID
		var idLen uint32
		binary.Read(reader, binary.LittleEndian, &idLen)
		idBytes := make([]byte, idLen)
		reader.Read(idBytes)
		id := string(idBytes)

		// 4. Read Vector
		var vecLen uint32
		binary.Read(reader, binary.LittleEndian, &vecLen)
		vecCount := vecLen / 4
		vector := make([]float32, vecCount)
		for i := 0; i < int(vecCount); i++ {
			binary.Read(reader, binary.LittleEndian, &vector[i])
		}

		// 5. Read Metadata
		var metaLen uint32
		binary.Read(reader, binary.LittleEndian, &metaLen)
		meta := make([]byte, metaLen)
		reader.Read(meta)

		// 6. Execute callback (Re-insert into DB)
		if op == OpInsert {
			fn(id, vector, meta)
		}
	}

	// Move pointer back to end for appending new writes
	wal.file.Seek(0, 2)
	return nil
}
