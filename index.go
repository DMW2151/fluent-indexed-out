package indexedlogplugin

import (
	"encoding/binary"
	"fmt"
	"os"
	"syscall"

	"github.com/google/btree"
	"github.com/google/uuid"
)

// IndexedLogFile - Manages the fileTree, uses all incoming log events to
// update the time-indexed btree, `index`
type IndexedLogFile struct {
	FID     uuid.UUID
	Index   *btree.BTree
	Options *LogFileOptions
}

// NewIndex - Create IndexedLogFile
func NewIndex(opt *LogFileOptions) *IndexedLogFile {

	// Create Node Index
	indexedFile := IndexedLogFile{
		Index:   btree.New(opt.TreeDepth),
		FID:     uuid.New(),
		Options: opt,
	}
	return &indexedFile
}

// NewIndexFromFile -
func NewIndexFromFile(fileID uuid.UUID, opt *LogFileOptions) *IndexedLogFile {

	var serIndex SerializedIndex

	// Open the index from disk...
	fi, _ := os.Open(
		fmt.Sprintf(`%s/%s.idx`, opt.Root, fileID),
	)
	defer fi.Close()

	err := binary.Read(fi, binary.BigEndian, &serIndex)
	if err != nil {
		// Handle
	}

	idxF := serIndex.Deserialize(opt)
	return &idxF
}

// Rotate - Rotate IndexedLogFile - in this context, `Rotate`, just means replace
// the UUID and generate a new file-tree
func (f *IndexedLogFile) Rotate() {
	f.FID = uuid.New()
	f.Index = btree.New(f.Options.TreeDepth)
}

// Serialize -
func (f *IndexedLogFile) Serialize() *SerializedIndex {

	var (
		// index, structSize - index object with a fixed size (indexSize) - will
		// be serialized to disk
		index = SerializedIndex{
			Lbound: nodeFromItem(f.Index.Min()).Timestamp,
			Ubound: nodeFromItem(f.Index.Max()).Timestamp,
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

	return &index
}

// Flush - serialize and write the current `IndexedLogFile` tree to the tail
// of the index file on disk...
func (f *IndexedLogFile) Flush() error {

	// Serialize
	index := f.Serialize()

	// Open the index-file for reading | writing ...
	fi, _ := os.OpenFile(
		fmt.Sprintf(`%s/%s.idx`, f.Options.Root, f.FID),
		os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644,
	)

	defer func() {
		fi.Sync()
		fi.Close()
	}()

	// Write whole index to file...
	err := binary.Write(fi, binary.BigEndian, index)

	return err
}

// FirstLTEHead
func (f *IndexedLogFile) FirstLTEHead(t int64) (offset int64) {

	// Starting from startNode - descend 1 node...
	var startNode = Node{
		Timestamp: t,
		Offset:    0,
	}

	var descIterFunc = func(item btree.Item) bool {
		n0 := nodeFromItem(item)
		isLess := (n0.Timestamp <= startNode.Timestamp)

		if isLess {
			startNode.Offset = n0.Offset
			return false
		}
		return true
	}

	f.Index.DescendLessOrEqual(&startNode, descIterFunc)

	return startNode.Offset
}

// FirstGTETail -
func (f *IndexedLogFile) FirstGTETail(t int64) (offset int64) {

	// Starting from startNode - ascend 1 node...
	var endNode = Node{
		Timestamp: t,
		Offset:    0,
		Length:    0,
	}

	var ascIterFunc = func(item btree.Item) bool {
		n0 := nodeFromItem(item)
		isGreater := (n0.Timestamp >= endNode.Timestamp)

		if isGreater {
			endNode.Offset = n0.Offset
			endNode.Length = n0.Length
			return false
		}
		return true
	}

	f.Index.AscendGreaterOrEqual(&endNode, ascIterFunc)

	// fallback to
	if endNode.Offset+int64(endNode.Length) == 0 {
		n0 := nodeFromItem(f.Index.Max())
		return n0.Offset + int64(n0.Length)
	}

	return endNode.Offset + int64(endNode.Length)
}

// OpenBetween -
func (f *IndexedLogFile) OpenBetweenPositions(offset int64, until int64) ([]byte, error) {

	var ps = int64(os.Getpagesize())

	// Open logfile as O_RDONLY...
	fi, err := os.OpenFile(
		fmt.Sprintf(`%s/%s.log`, f.Options.Root, f.FID), os.O_RDONLY, 0644,
	)
	if err != nil {
		fmt.Println(err)
	}
	defer fi.Close()

	// if offset doesn't go evenly into os.PageSize
	// then modify s.t. it does...
	offset = ps * (offset / ps)

	// MMap the file based on the bounds derived from tree traversal
	mmap, err := syscall.Mmap(
		int(fi.Fd()), offset, int(until-offset),
		syscall.PROT_READ, syscall.MAP_PRIVATE,
	)

	// NOTE: There are a lot of errors that can show up here w. a poor
	// error message
	if err != nil {
		return []byte{}, err
	}

	// Filter to exact location
	// TBD...

	return mmap, nil
}
