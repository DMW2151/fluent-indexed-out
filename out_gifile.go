package main

import (
	"C"
	"fmt"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "gifile", "indexedFile")
}

//export FLBPluginInit
// (fluentbit will call this)
// plugin (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(plugin unsafe.Pointer) int {
	// Example to retrieve an optional configuration parameter
	param := output.FLBPluginConfigKey(plugin, "param")
	fmt.Printf("[flb-go] plugin parameter = '%s'\n", param)
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {

	var (
		count     int
		ret       int
		ts        interface{}
		record    map[interface{}]interface{}
		timestamp time.Time
	)

	// Handle Creation (or Append) to LogFile...

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	// Iterate Records
	count = 0
	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		switch t := ts.(type) {
		case output.FLBTime:
			timestamp = ts.(output.FLBTime).Time
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			fmt.Println("time provided invalid, defaulting to now.")
			timestamp = time.Now()
		}
		// Write Records to the
		// Print record keys and values
		fmt.Printf(
			"[%d] %s: [%s, {",
			count,
			C.GoString(tag),
			timestamp.String(),
		)

		for k, v := range record {
			fmt.Printf("\"%s\": %v, ", k, v)
		}

		fmt.Printf("}\n")
		count++

		// Write Node Breakpoint iff > pages threshold
		// f.tree.ReplaceOrInsert(&Node{
		// 	Offset:    (i * f.pageSize),
		// 	Length:    f.pageSize,
		// 	Timestamp: t,
		// })

	}

	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	return output.FLB_OK
}

func main() {
}
