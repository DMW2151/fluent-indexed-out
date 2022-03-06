package indexedlogplugin

import (
	"encoding/json"

	"github.com/google/btree"
)

// Node - An element in the FileTree object - represents a single chunk of the file
// that can be used as leaf in tree / time-anchorpoint in file
type Node struct {
	Offset    int64
	Length    int32
	Timestamp int64
}

// Less - tests whether the current item is less than the given argument.
// required s.t. Node can implement the btree.Item interface
func (n *Node) Less(item btree.Item) bool {
	n0 := nodeFromItem(item)
	return n.Timestamp < n0.Timestamp
}

// nodeFromItem - create a Node from btree.Item (in context almost always
// just a node w. a item wrapper...)
func nodeFromItem(item btree.Item) (n0 Node) {
	b, _ := json.Marshal(item)
	_ = json.Unmarshal(b, &n0)
	return n0
}
