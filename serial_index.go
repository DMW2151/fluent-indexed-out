package indexedlogplugin

import (
	"github.com/google/btree"
)

const nodesPerFile = 256

// SerializedIndex - Summarizes an `IndexedLogFile` w. the bounds and nodes of
// represented as  fixed length types. Easily writeable/readable as binary...
type SerializedIndex struct {

	// Lbound, Ubound - Upper and Lower Bound of the File...
	Lbound, Ubound int64

	// Nodes - All nodes in the `IndexedLogFile` tree...
	Nodes [nodesPerFile]Node

	// fID - A UUID representing the location of the `IndexedLogFile`'s actual
	// entries. This index references the entries in `${ROOT}/${UUID}.log`
	FID [16]byte
}

// Deserialize -
func (sI *SerializedIndex) Deserialize(opt *LogFileOptions) IndexedLogFile {

	indexedFile := IndexedLogFile{
		Index:   btree.New(opt.TreeDepth),
		FID:     sI.FID,
		Options: opt,
	}

	for i, n := range sI.Nodes {
		if (n.Timestamp > 0) && (i <= nodesPerFile) {
			indexedFile.Index.ReplaceOrInsert(&sI.Nodes[i])
		}
	}

	return indexedFile

}
