package main

import (
	"C"
	"fmt"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

import (
	"encoding/json"
	"io"
	"os"

	fio "github.com/dmw2151/fluent-indexed-out"
)

// Const...
const (
	bytesPerNode int64 = 1 * 1024 * 32
	nodesPerFile       = 1024
)

var (
	opt = fio.LogFileOptions{
		BytesPerNode: bytesPerNode,
		Root:         `/tmp`,
		TreeDepth:    2,
	}

	cw      = &fio.CounterWr{}
	encoder = json.NewEncoder(cw)
	logFile = fio.NewIndex(&opt)
)

// roundToFloorMultiple
func roundToFloorMultiple(n int64, m int) int64 {
	return (n / int64(m)) * int64(m)
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "go-indexed-file", "go-indexed-file")
}

//export FLBPluginInit
// (fluentbit will call this)
// plugin (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(plugin unsafe.Pointer) int {
	// Example to retrieve an optional configuration parameter
	param := output.FLBPluginConfigKey(plugin, "param")
	fmt.Println(
		fmt.Sprintf("[go-indexed-file] plugin parameter = '%s'\n", param),
	)
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {

	var timestamp time.Time

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	fi, _ := os.OpenFile(
		fmt.Sprintf("%s/%s.log", logFile.Options.Root, logFile.FID),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644,
	)
	defer fi.Close()

	// Seek End...no clue where might be...
	fi.Seek(0, io.SeekEnd)

	cw.Writer = fi

	for {
		// Extract Record
		// See: https://github.com/fluent/fluent-bit-go/blob/7785f07f38f4c6ec1dd139c7ee0fa89af92187f9/output/decoder.go#L71
		ret, ts, records := output.GetRecord(dec)

		// Unexpectedly, this is a critical line...
		if ret != 0 {
			break
		}

		// NOTE: time is an interface{}, can be double or FLBTime
		switch t := ts.(type) {
		case output.FLBTime:
			timestamp = ts.(output.FLBTime).Time
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			timestamp = time.Now()
		}

		// Range over map[interface{}]interface{}, encoding each `record` as a
		// new entry to logfile...
		for k, v := range records {

			// Write JSON...
			err := encoder.Encode(&fio.Record{
				Timestamp: timestamp.UTC(),
				Tag:       C.GoString(tag),
				Key:       k,
				Value:     v,
			})

			// TODO: Handle...
			if err != nil {
				fmt.Println(err)
			}

			// BUG: What do we do if there aren't many records!? this increases the delay
			// to hit the index!!
			//
			// Write Node Breakpoint iff the total bytes written crosses K * bytesPerNode...
			if cw.CurBytesWritten > int(logFile.Options.BytesPerNode) {

				// Add Node w the following properties. These ensure that all nodes'
				// offsets can be Seek'd too and all nodes will end on a clean line
				// break
				//
				// 	- Offset as a multiple of bytesPerNode
				// 	- Length as any int64...
				stdByteOffset := roundToFloorMultiple(
					cw.Offset+cw.BytesWritten,
					int(logFile.Options.BytesPerNode),
				)

				// BUG: Consider putting an increasing jitter here to ensure no duplicate
				// node writes...
				logFile.Index.ReplaceOrInsert(&fio.Node{
					Offset: stdByteOffset,
					Length: int32(
						(cw.Offset + cw.BytesWritten) - stdByteOffset,
					),
					Timestamp: timestamp.UnixNano(),
				})

				// Reset the bytesWritten to the Node to 0!
				cw.CurBytesWritten = 0
				logFile.Flush()

				// Check that we are not starting a new overflowing node group...
				if logFile.Index.Len() > (nodesPerFile - 1) {

					// Rotate
					logFile.Rotate()

					// Open logfile generated from logFile.Rotate()...
					fi, _ := os.OpenFile(
						fmt.Sprintf("%s/%s.log", logFile.Options.Root, logFile.FID),
						os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644,
					)
					defer fi.Close()

					cw.Writer = fi

				}

			}
		}
	}

	// See FLB Output Docs, one of:
	//	- (output.FLB_OK, output.FLB_ERROR, output.FLB_RETRY)
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
