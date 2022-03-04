package main

import (
	"time"

	fio "github.com/dmw2151/fluent-indexed-out"
)

const (
	pageSize     int32 = 1 * 1024 * 32
	nodesPerFile       = 4
)

var (
	opt = fio.LogFileOptions{
		PageSize:  pageSize,
		Root:      `./tmp`,
		TreeDepth: 2,
	}

	h *fio.IndexedLogFile = fio.NewLogFile(&opt)
)

func main() {

	for i := 0; i < 2; i++ {

		h.Rotate()

		for j := 0; j < 2; j++ {

			h.Index.ReplaceOrInsert(&fio.Node{
				Offset:    4096,
				Length:    4096,
				Timestamp: time.Now().UnixNano(),
			})

			h.Flush(j == 0)
		}
	}
	//h.ReadIndex(1)
}
