package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWAL(t *testing.T) {
	// Create a temporary directory for the test
	tmpDir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	walPath := filepath.Join(tmpDir, "test.wal")

	// Open WAL
	wal, err := OpenWal(walPath)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	// Test writing an insert entry
	loc1 := FileLocation{Offset: 100, Length: 50}
	vector1 := []float32{1.0, 2.0, 3.0}
	meta1 := []byte("metadata1")
	err = wal.WriteEntry(OpInsert, "id1", vector1, meta1, loc1)
	if err != nil {
		t.Fatal(err)
	}

	// Test writing a delete entry (though Recover ignores it)
	loc2 := FileLocation{Offset: 200, Length: 30}
	vector2 := []float32{4.0, 5.0}
	meta2 := []byte("metadata2")
	err = wal.WriteEntry(OpDelete, "id2", vector2, meta2, loc2)
	if err != nil {
		t.Fatal(err)
	}

	// Test writing another insert
	loc3 := FileLocation{Offset: 300, Length: 40}
	vector3 := []float32{6.0, 7.0, 8.0, 9.0}
	meta3 := []byte("metadata3")
	err = wal.WriteEntry(OpInsert, "id3", vector3, meta3, loc3)
	if err != nil {
		t.Fatal(err)
	}

	// Recover and collect entries
	var recovered []struct {
		id     string
		vector []float32
		meta   []byte
		loc    FileLocation
	}
	fn := func(id string, vector []float32, meta []byte, loc FileLocation) {
		recovered = append(recovered, struct {
			id     string
			vector []float32
			meta   []byte
			loc    FileLocation
		}{id, vector, meta, loc})
	}
	err = wal.Recover(fn)
	if err != nil {
		t.Fatal(err)
	}

	// Verify recovered entries (only inserts should be recovered)
	expected := []struct {
		id     string
		vector []float32
		meta   []byte
		loc    FileLocation
	}{
		{"id1", []float32{1.0, 2.0, 3.0}, []byte("metadata1"), FileLocation{Offset: 100, Length: 50}},
		{"id3", []float32{6.0, 7.0, 8.0, 9.0}, []byte("metadata3"), FileLocation{Offset: 300, Length: 40}},
	}

	if len(recovered) != len(expected) {
		t.Fatalf("expected %d entries, got %d", len(expected), len(recovered))
	}

	for i, exp := range expected {
		rec := recovered[i]
		if rec.id != exp.id {
			t.Errorf("entry %d: expected id %s, got %s", i, exp.id, rec.id)
		}
		if len(rec.vector) != len(exp.vector) {
			t.Errorf("entry %d: expected vector len %d, got %d", i, len(exp.vector), len(rec.vector))
		} else {
			for j, v := range exp.vector {
				if rec.vector[j] != v {
					t.Errorf("entry %d: vector[%d] expected %f, got %f", i, j, v, rec.vector[j])
				}
			}
		}
		if string(rec.meta) != string(exp.meta) {
			t.Errorf("entry %d: expected meta %s, got %s", i, string(exp.meta), string(rec.meta))
		}
		if rec.loc != exp.loc {
			t.Errorf("entry %d: expected loc %+v, got %+v", i, exp.loc, rec.loc)
		}
	}
}
