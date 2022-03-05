package indexedLogPlugin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/google/btree"
	"github.com/google/uuid"
)

const nodesPerFile = 1024

// LogFileOptions - Options passed to `IndexedLogFile` to define index
// writing patterns
type LogFileOptions struct {
	PageSize  int32
	Root      string
	TreeDepth int
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
		Ubound:  time.Now().UnixNano(),
	}
	return &indexedFile
}

// Rotate - Rotate IndexedLogFile - in this context, `Rotate`, just means replace
// the UUID and generate a new file-tree
func (f *IndexedLogFile) Rotate() {

	// Reset the UUID and btree...
	f.FID = uuid.New()
	f.Index = btree.New(f.Options.TreeDepth)

	// Rotate the time bounds s.t. this file's bounds directly follow the
	// bounds of the old file...
	f.Lbound = f.Ubound
	f.Ubound = time.Now().UnixNano()
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

		// bwr -  Intermediate buffer between mmap and tree
		bwr bytes.Buffer
	)

	// Add UUID to SerializedIndex
	copy(index.FID[:], f.FID[:])

	// Add Nodes to SerializedIndex
	f.Index.Ascend(nodeDumpIter)

	// Open the index-file for reading | writing ...
	fi, _ := os.OpenFile(
		fmt.Sprintf(`%s/fluent-index`, f.Options.Root),
		os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644,
	)
	defer fi.Close()

	// On first write, where there is no content to mmap & modify...
	if firstWrite {

		// Add activeNodes into index.Nodes...
		copy(index.Nodes[:], activeNodes)

		// Write to file...
		_, _ = fi.Seek(0, io.SeekEnd)
		err := binary.Write(fi, binary.BigEndian, index)
		return err
	}

	// On subsequent writes, mmap the last linux page of the file and modify the
	// last `SerializedIndex`. For example: In a 12288 byte file, given page sizes
	// of 4096 and an indexSize of 200, read [8192, 12888], modify [12688, 12888]

	// Get indexLen s.t. we can offset into a valid value...
	indexLen, _ := fi.Seek(0, io.SeekEnd)

	// MMap the section of the file described above
	// BUG: Ensure the bounds for this section are correct!
	mmap, err := syscall.Mmap(
		int(fi.Fd()),
		0,
		int(indexLen),
		syscall.PROT_WRITE|syscall.PROT_READ,
		syscall.MAP_SHARED,
	)
	defer syscall.Munmap(mmap)

	if err != nil {
		return err
	}

	// Replace Upper Bound && Nodes: The names of the fields dictate
	// the order of the bytes in the struct! careful!
	//
	// Recall: We have the following format given the defn of
	// `SerializedIndex`
	// {
	//	 1646453905411859000  - BoundHigh
	//	 1646453905202542000  - BoundLow
	//	 [{4096 4096 1646453905202542000} {0 0 0} {0 0 0} {0 0 0}]  - Nodes
	//	 [251 115 87 119 103 114 74 38 159 20 94 38 210 161 122 165] - UUID
	// }

	// BoundHigh occupies [offset, offset + 8]
	binary.BigEndian.PutUint64(
		mmap[:9], uint64(index.BoundHigh),
	)

	// Nodes occupy [offset + 17, offset + 17 + (nodesPerFile * nodeSize)]
	// nodeSize == 20 Bytes
	//	- Offset, Timestamp == 8 ea.
	// 	- Length == 4
	_ = binary.Write(&bwr, binary.BigEndian, activeNodes)
	copy(mmap[16:], bwr.Bytes())

	return nil

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
		Length:    f.Options.PageSize,
		Offset:    0,
	}

	// EndNode - Used for Setting Upper Bound
	var endNode = Node{
		Timestamp: eTime.Unix(),
		Length:    f.Options.PageSize,
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
