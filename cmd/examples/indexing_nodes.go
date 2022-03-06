package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	fio "github.com/dmw2151/fluent-indexed-out"
)

var (
	opt = fio.LogFileOptions{
		BytesPerNode: 1 * 1024 * 32,
		Root:         `./tmp`,
		TreeDepth:    2,
	}

	idxF *fio.IndexedLogFile = fio.NewIndex(&opt)

	seed int64 = 2151
)

func main() {

	// Set Seed
	src := rand.NewSource(seed)
	gen := rand.New(src)

	// Fill tree to some fraction of capacity...
	for n := 0; n < 256; n++ {

		// In practice - this should reject nodes beyond ${nodesPerFile}
		idxF.Index.ReplaceOrInsert(&fio.Node{
			Offset:    gen.Int63n(1 << 16),
			Length:    4096 + gen.Int31n(1<<8),
			Timestamp: time.Now().UnixNano(),
		})
	}

	// Write to ./tmp/${UUID}.idx
	err := idxF.Flush()

	if err != nil {
		log.Panic(err)
	}

	// Restore from ./tmp/${UUID}.idx
	idxRestored := fio.NewIndexFromFile(idxF.FID, &opt)

	// Compare
	fmt.Println(
		idxRestored.Index.Min(), idxF.Index.Min(),
		idxRestored.Index.Max(), idxF.Index.Max(),
	)

}
