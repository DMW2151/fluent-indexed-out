package main

import (
	"io"
)

// Record - ...
type Record struct {
	Timestamp string
	Tag       string
	Key       interface{}
	Value     interface{}
}

// CounterWr - https://stackoverflow.com/a/37704940
type CounterWr struct {
	Writer               io.Writer
	Offset, BytesWritten int64
	CurBytesWritten      int
}

func (cw *CounterWr) Write(p []byte) (n int, err error) {
	n, err = cw.Writer.Write(p)
	cw.BytesWritten += int64(n)
	cw.CurBytesWritten += n
	return
}

// roundToFloorMultiple
func roundToFloorMultiple(n int64, m int) int64 {
	return (n / int64(m)) * int64(m)
}
