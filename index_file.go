package indexedLogPlugin

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/google/btree"
	"github.com/google/uuid"
)

const nodesPerFile = 4

// LogFileOptions - Options passed to `IndexedLogFile` to define index
// writing patterns
type LogFileOptions struct {
	BytesPerNode int32
	Root         string
	TreeDepth    int
}

// IndexedLogFile - Manages the fileTree, uses all incoming log events to
// update the time-indexed btree, `index`
type IndexedLogFile struct {
	FID            uuid.UUID
	Index          *btree.BTree
	Options        *LogFileOptions
	Lbound, Ubound int64
}

// SerializedIndex - Summarizes an `IndexedLogFile` w. the bounds and nodes of
// represented as  fixed length types. Easily writeable/readable as binary...
type SerializedIndex struct {

	// Lbound, Ubound - Upper and Lower Bound of the File...
	BoundLow, BoundHigh int64

	// Nodes - All nodes in the `IndexedLogFile` tree...
	Nodes [nodesPerFile]Node

	// fID - A UUID representing the location of the `IndexedLogFile`'s actual
	// entries. This index references the entries in `${ROOT}/${UUID}.log`
	FID [16]byte
}

// IndexCollection - A collection of `IndexedLogFile` objects
// Expect (?) to be held in memory for queries against multiple files
type IndexCollection []*IndexedLogFile

// NewLogFile - Create IndexedLogFile
func NewLogFile(opt *LogFileOptions) *IndexedLogFile {

	// New UUID
	fID := uuid.New()

	// Create Node Index
	indexedFile := IndexedLogFile{
		Index:   btree.New(2),
		FID:     fID,
		Options: opt,
	}
	return &indexedFile
}

// Rotate - Rotate IndexedLogFile - in this context, `Rotate`, just means replace
// the UUID and generate a new file-tree
// Reset the UUID and btree...
func (f *IndexedLogFile) Rotate() {
	f.FID = uuid.New()
	f.Index = btree.New(f.Options.TreeDepth)
}

// Flush - serialize and write the current `IndexedLogFile` tree to the tail
// of the index file on disk...
func (f *IndexedLogFile) Flush(firstWrite bool) error {

	var (

		// index, structSize - index object with a fixed size (indexSize) - will
		// be serialized to disk
		index = SerializedIndex{
			BoundLow:  nodeFromItem(f.Index.Min()).Timestamp,
			BoundHigh: nodeFromItem(f.Index.Max()).Timestamp,
		}

		activeNodes = make([]Node, f.Index.Len())
		// i, nodeDumpIter - Iterate over the nodes in the current tree w.
		// nodeDumpIter, write to the serialized
		i            = 0
		nodeDumpIter = func(item btree.Item) bool {
			n0 := nodeFromItem(item)
			activeNodes[i] = n0
			i++
			return true
		}
	)

	// Add UUID to SerializedIndex
	copy(index.FID[:], f.FID[:])

	// Add activeNodes into index.Nodes...
	f.Index.Ascend(nodeDumpIter)
	copy(index.Nodes[:], activeNodes)

	// Open the index-file for reading | writing ...
	fi, _ := os.OpenFile(
		fmt.Sprintf(`%s/fluent-index`, f.Options.Root),
		os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644,
	)
	defer fi.Close()

	// Write to file...
	if firstWrite {
		fi.Seek(0, io.SeekEnd)
	} else {
		pos, _ := fi.Seek(
			-int64(binary.Size(index)), io.SeekEnd,
		)

		fi.Truncate(pos)
	}

	err := binary.Write(fi, binary.BigEndian, index)

	return err

}

// ReadIndex -
func (f *IndexedLogFile) ReadIndex(numIndexes int) (ic []*IndexCollection) {

	var (
		index      SerializedIndex
		structSize int = binary.Size(index)
	)

	// Open the index from disk...
	fi, _ := os.Open(
		fmt.Sprintf(`%s/fluent-index`, f.Options.Root),
	)
	defer fi.Close()

	// Calculate the number of distinct file indexes there are
	// i.e. How many times has the file been rotated?
	l, _ := fi.Seek(0, io.SeekEnd)
	nBlocks := int(l) / structSize

	// Seek `numIndexes`
	_, _ = fi.Seek(
		-1*int64(numIndexes*structSize), io.SeekEnd,
	)

	// Read the last `numIndexes`
	for r := (nBlocks - numIndexes); r < nBlocks; r++ {
		_ = binary.Read(fi, binary.BigEndian, &index)
		fmt.Println(
			fmt.Sprintf(`%d, %v`, r, index),
		)
	}
	return

}

// OpenBetween -
func (f *IndexedLogFile) OpenBetween(sTime, eTime time.Time) ([]byte, error) {

	// startNode - Used for Setting Lower Bound
	var startNode = Node{
		Timestamp: sTime.Unix(),
		Length:    f.Options.BytesPerNode,
		Offset:    0,
	}

	// EndNode - Used for Setting Upper Bound
	var endNode = Node{
		Timestamp: eTime.Unix(),
		Length:    f.Options.BytesPerNode,
	}

	// descIterFunc -
	var descIterFunc = func(item btree.Item) bool {
		n0 := nodeFromItem(item)

		if n0.Timestamp <= startNode.Timestamp {
			startNode.Offset = n0.Offset + int64(n0.Length)
			return false
		}
		return true
	}

	// ascIterFunc -
	var ascIterFunc = func(item btree.Item) bool {
		n0 := nodeFromItem(item)
		if n0.Timestamp >= endNode.Timestamp {
			endNode = n0
			return false
		}
		return true
	}

	// Open logfile as O_RDONLY...
	fi, _ := os.OpenFile(
		fmt.Sprintf(`./%s`, f.FID),
		os.O_RDONLY, 0644,
	)
	defer fi.Close()

	// Query the tree to get bounds - iterate between startNode and endNode
	// AscendRange modifies upper and lower bounds...
	f.Index.DescendLessOrEqual(&startNode, descIterFunc)
	f.Index.AscendGreaterOrEqual(&endNode, ascIterFunc)

	// MMap the file based on the bounds derived from tree traversal
	mmap, err := syscall.Mmap(
		int(fi.Fd()),
		startNode.Offset,
		int(
			int64(endNode.Offset)+int64(endNode.Length)-startNode.Offset,
		),
		syscall.PROT_READ,
		syscall.MAP_PRIVATE,
	)

	// NOTE: There are a lot of errors that can show up here w. a poor
	// error message
	if err != nil {
		return []byte{}, err
	}

	return mmap, nil
}
