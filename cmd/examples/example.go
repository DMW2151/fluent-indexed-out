package main

import (
	"fmt"
	"time"

	fio "github.com/dmw2151/fluent-indexed-out"
)

const (
	bytesPerNode int32 = 1 * 1024 * 32
)

var (
	opt = fio.LogFileOptions{
		BytesPerNode: bytesPerNode,
		Root:         `./tmp`,
		TreeDepth:    2,
	}

	h   *fio.IndexedLogFile = fio.NewLogFile(&opt)
	err error
)

// Tests to consider

// Performance on full tree
// Performance on half tree
// Performance on 1-node tree
// Performance on 1024 tree
// Performance on 8192 tree
// MultiFlush...
// Add more nodes than file stated capacity??

func main() {

	for i := 0; i < 1; i++ {

		h.Rotate()

		for j := 0; j < 1; j++ {

			// Fill tree to some fraction of capacity...
			for n := 0; n < 512; n++ {
				h.Index.ReplaceOrInsert(&fio.Node{
					Offset:    4096,
					Length:    4096,
					Timestamp: time.Now().UnixNano(),
				})
			}

			err = h.Flush(j == 0)
			if err != nil {
				fmt.Println(err)
			}

		}
	}

	h.ReadIndex(1)
}
