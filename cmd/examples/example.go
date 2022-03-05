package main

import (
	"fmt"
	"time"

	fio "github.com/dmw2151/fluent-indexed-out"
)

const (
	pageSize int32 = 1 * 1024 * 32
)

var (
	opt = fio.LogFileOptions{
		PageSize:  pageSize,
		Root:      `./tmp`,
		TreeDepth: 2,
	}

	h *fio.IndexedLogFile = fio.NewLogFile(&opt)
)

// Tests to consider

// Performance on full tree
// Performance on half tree
// Performance on 1-node tree
// Performance on 1024 tree
// Performance on 8192 tree

func main() {

	for i := 0; i < 1; i++ {

		h.Rotate()

		for j := 0; j < 2; j++ {

			// Fill tree to some fraction of capacity...
			for n := 0; n < 512; n++ {
				h.Index.ReplaceOrInsert(&fio.Node{
					Offset:    4096,
					Length:    4096,
					Timestamp: time.Now().UnixNano(),
				})
			}

			fmt.Println(time.Now().UnixNano())
			for k := 0; k < 100; k++ {
				h.Flush(j == 0)
			}
			fmt.Println(time.Now().UnixNano())

		}
	}
}
