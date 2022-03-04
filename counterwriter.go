package indexedLogPlugin

import "io"

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
