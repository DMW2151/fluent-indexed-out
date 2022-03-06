package indexedlogplugin

import (
	"crypto/md5"
	"encoding/binary"
	"io/ioutil"
	"math/rand"

	"github.com/google/uuid"
)

// ioCopy
func ioCopy(src, dst string) (err error) {

	input, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(dst, input, 0644)
	if err != nil {
		return err
	}
	return
}

// deterministicUUIDFromString
func deterministicUUIDFromString(fn string) uuid.UUID {

	// Create deterministic UUID
	fnHash := md5.Sum([]byte(fn))

	uintHash := int64(binary.BigEndian.Uint64(fnHash[:]))

	rnd := rand.New(
		rand.NewSource(uintHash),
	)

	uuid.SetRand(rnd)
	u, _ := uuid.NewRandomFromReader(rnd)
	return u
}
