package main

import (
	"os"
	"syscall"
	"time"

	"github.com/google/btree"
)

// FileOptions -
type FileOptions struct {
	pageSize int64
}

// IndexedLogFile -
type IndexedLogFile struct {
	logfile string
	index   *btree.BTree
	options FileOptions
}

// Open -
func (f *IndexedLogFile) Open(sTime, eTime time.Time) ([]byte, error) {

	// startNode - Used for Setting Lower Bound
	var startNode = Node{
		Timestamp: sTime.Unix(),
		Length:    f.options.pageSize,
		Offset:    0,
	}

	// EndNode - Used for Setting Upper Bound
	var endNode = Node{
		Timestamp: eTime.Unix(),
		Length:    f.options.pageSize,
	}

	// descIterFunc -
	var descIterFunc = func(item btree.Item) bool {
		n0 := nodeFromItem(item)

		if n0.Timestamp <= startNode.Timestamp {
			startNode.Offset = (n0.Offset + n0.Length)
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
	fi, _ := os.OpenFile(f.logfile, os.O_RDONLY, 0644)
	defer fi.Close()

	// Query the tree to get bounds - iterate between startNode and endNode
	// AscendRange modifies upper and lower bounds...
	f.index.DescendLessOrEqual(&startNode, descIterFunc)
	f.index.AscendGreaterOrEqual(&endNode, ascIterFunc)

	// MMap the file based on the bounds derived from tree traversal
	b, err := syscall.Mmap(
		int(fi.Fd()),
		startNode.Offset,
		int((endNode.Offset+endNode.Length)-startNode.Offset),
		syscall.PROT_READ,
		syscall.MAP_PRIVATE,
	)

	// NOTE: There are a lot of errors that can show up here w. a poor
	// error message
	if err != nil {
		return []byte{}, err
	}

	return b, nil
}
