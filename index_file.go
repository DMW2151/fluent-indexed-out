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

const nodesPerFile = 4

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
	FID     uuid.UUID
	Index   *btree.BTree
	Options *LogFileOptions
}

// SerializedIndex - Summarizes an `IndexedLogFile` w. the bounds and nodes of
// represented as  fixed length types. Easily writeable/readable as binary...
type SerializedIndex struct {

	// Lbound, Ubound - Upper and Lower Bound of the File...
	Lbound, Ubound int64

	// Nodes - All nodes in the `IndexedLogFile` tree...
	Nodes [nodesPerFile]Node

	// fID - A UUID representing the location of the `IndexedLogFile`'s actual
	// entries. This index references the entries in `${ROOT}/${UUID}.log`
	fID [16]byte
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
			Lbound: nodeFromItem(f.Index.Min()).Timestamp,
			Ubound: nodeFromItem(f.Index.Max()).Timestamp,
		}
		indexSize = binary.Size(index)

		// i, nodeDumpIter - Iterate over the nodes in the current tree w.
		// nodeDumpIter, write to the serialized
		i            = 0
		nodeDumpIter = func(item btree.Item) bool {
			n0 := nodeFromItem(item)
			index.Nodes[i] = n0
			i++
			return true
		}

		// bwr -  Intermediate buffer between mmap and tree
		bwr bytes.Buffer
	)

	// Add UUID to SerializedIndex
	b, err := f.FID.MarshalBinary()
	if err != nil {
		return err
	}
	copy(index.fID[:], b)

	// Add Nodes to SerializedIndex
	f.Index.Ascend(nodeDumpIter)

	// Open the index-file for reading | writing ...
	fi, _ := os.OpenFile(
		fmt.Sprintf(`%s/fluent-index`, f.Options.Root),
		os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644,
	)
	defer fi.Close()

	// Seek to the end of the file & grab the length

	// On first write, where there is no content to mmap & modify, seek to end
	// and write the index
	if firstWrite {
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

	// TODO/BUG: This can be much faster using an unsafePtr (?) - might not
	// matter much though...
	binary.Write(&bwr, binary.BigEndian, index)
	copy(
		mmap[(int(indexLen)-indexSize):],
		bwr.Bytes(),
	)

	return nil

}

// ReadIndex -
func (f *IndexedLogFile) ReadIndex(lastNBlocks int) (ic []*IndexCollection) {

	var (
		index      SerializedIndex
		structSize int = binary.Size(index)
	)

	fi, _ := os.Open(
		fmt.Sprintf(`%s/fluent-index`, f.Options.Root),
	)
	defer fi.Close()

	l, _ := fi.Seek(0, io.SeekEnd)
	nBlocks := int(l) / structSize

	pos, _ := fi.Seek(
		-1*int64(lastNBlocks*structSize), io.SeekEnd,
	)

	// Read the last `lastNBlocks`
	for r := (nBlocks - lastNBlocks); r < nBlocks; r++ {
		fmt.Println(
			fmt.Sprintf(`%d, %v`, pos, index),
		)
		_ = binary.Read(fi, binary.BigEndian, &index)
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
