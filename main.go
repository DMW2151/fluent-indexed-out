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

	"github.com/google/btree"
)

//
const pageSize int = 1 * 1024 * 32

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

	// Create Logfile
	fi, _ := os.OpenFile("/tmp/file.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	defer fi.Close()

	fi.Seek(0, io.SeekEnd)

	// Create LogFile Writer
	cw := &CounterWr{Writer: fi}
	encoder := json.NewEncoder(cw)

	// Create Node File
	f := IndexedLogFile{
		index:     btree.New(2),
		logfile:   fi.Name(),
		indexfile: "node-001.idx",
		options: &FileOptions{
			pageSize: pageSize,
		},
	}

	for {
		// Extract Record
		// See: https://github.com/fluent/fluent-bit-go/blob/7785f07f38f4c6ec1dd139c7ee0fa89af92187f9/output/decoder.go#L71
		ret, ts, records := output.GetRecord(dec)

		// Unexpectedly - Critical Line Here...
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

			// Write
			err := encoder.Encode(&Record{
				Timestamp: timestamp.String(),
				Tag:       C.GoString(tag),
				Key:       k,
				Value:     v,
			})

			// TODO: Handle...
			if err != nil {
				fmt.Println(err)
			}

			// Write Node Breakpoint iff the total bytes written crosses K * pageSize...
			if cw.CurBytesWritten > f.options.pageSize {

				// Add Node w the following properties. These ensure that all nodes'
				// offsets can be Seek'd too and all nodes will end on a clean line
				// break
				//
				// 	- Offset as a multiple of pagesize
				// 	- Length as any int64...
				stdByteOffset := roundToFloorMultiple(
					cw.Offset+cw.BytesWritten,
					f.options.pageSize,
				)

				f.index.ReplaceOrInsert(&Node{
					Offset: stdByteOffset,
					Length: int(
						(cw.Offset + cw.BytesWritten) - stdByteOffset,
					),
					Timestamp: timestamp.Unix(),
				})

				// Reset the bytesWritten to the Node to 0!
				cw.CurBytesWritten = 0
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
