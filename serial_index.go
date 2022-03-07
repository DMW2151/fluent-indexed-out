package indexedlogplugin

import (
	"bytes"
	"encoding/binary"
	"os"

	"github.com/google/btree"
)

const NODESPERFILE = 4096

// SerializedIndex - Summarizes an `IndexedLogFile` w. the bounds and nodes of
// represented as  fixed length types. Easily writeable/readable as binary...
type SerializedIndex struct {

	// fID - A UUID representing the location of the `IndexedLogFile`'s actual
	// entries. This index references the entries in `${ROOT}/${UUID}.log`
	FID [16]byte

	// Nodes - All nodes in the `IndexedLogFile` tree...
	Nodes [NODESPERFILE]Node
}

// Deserialize -
func (sI *SerializedIndex) Deserialize(opt *LogFileOptions) IndexedLogFile {

	indexedFile := IndexedLogFile{
		Index:   btree.New(opt.TreeDepth),
		FID:     sI.FID,
		Options: opt,
	}

	for i, n := range sI.Nodes {
		if (n.Timestamp > 0) && (i <= NODESPERFILE) {
			indexedFile.Index.ReplaceOrInsert(&sI.Nodes[i])
		}
	}

	return indexedFile
}

func (sI *SerializedIndex) SafeBytes() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, sI)
	return buf.Bytes()
}

func (sI *SerializedIndex) NodeSafeBytes() []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, sI.Nodes)
	return buf.Bytes()
}

// ReadSerializedIndex -
func ReadSerializedIndex(path string) (serIndex SerializedIndex, err error) {

	// Open the index file from disk && read whole contents
	fi, err := os.Open(path)

	if err != nil {
		return serIndex, err
	}
	defer fi.Close()

	// NOTE: hinary.Read() ~200x slower than doing the byte order
	// mappings but still *very* fast
	err = binary.Read(fi, binary.BigEndian, &serIndex)
	if err != nil {
		return serIndex, err
	}

	return serIndex, nil
}
