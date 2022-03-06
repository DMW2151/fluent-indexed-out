package main

import (
	"fmt"
	"math/rand"
	"time"

	fio "github.com/dmw2151/fluent-indexed-out"
)

var (
	opt = fio.LogFileOptions{
		BytesPerNode: 1 * 1024 * 4,
		Root:         `./tmp`,
		TreeDepth:    2,
	}

	idxF *fio.IndexedLogFile = fio.NewIndex(&opt)

	// Set Seed
	seed int64 = 2151

	// Set believable start variables
	offset int64 = 0
	length int32 = int32(opt.BytesPerNode)
)

func main() {

	// Set Seed
	src := rand.NewSource(seed)
	gen := rand.New(src)

	// Fill tree to some fraction of capacity...
	for n := 0; n < 128; n++ {
		// In practice - this should reject nodes beyond ${nodesPerFile}
		idxF.Index.ReplaceOrInsert(&fio.Node{
			Offset:    offset,
			Length:    length,
			Timestamp: time.Now().UnixNano(),
		})

		// Update values w. random offsets...
		offset = offset + int64(length)
		length = int32(opt.BytesPerNode) + gen.Int31n(1<<8)
	}

	// Write to ./tmp/${UUID}.idx
	n, err := idxF.Flush()
	fmt.Println(n)

	// Fill tree to some fraction of capacity...
	for n := 0; n < 12; n++ {
		idxF.Index.ReplaceOrInsert(&fio.Node{
			Offset:    offset,
			Length:    length,
			Timestamp: time.Now().UnixNano(),
		})
		offset = offset + int64(length)
		length = int32(opt.BytesPerNode) + gen.Int31n(1<<8)
	}

	n, err = idxF.Flush()
	fmt.Println(n)

	if err != nil {
		fmt.Println(err)
	}

	// Restore from ./tmp/${UUID}.idx
	serIndex, err := fio.ReadSerializedIndex(idxF.FID, &opt)
	if err != nil {
		fmt.Println(err)
	}

	idxRestored := serIndex.Deserialize(&opt)

	// Compare
	fmt.Println(idxF.Index.Min(), idxF.Index.Max())
	fmt.Println(idxRestored.Index.Min(), idxRestored.Index.Max())

}
