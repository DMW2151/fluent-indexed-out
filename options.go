package indexedlogplugin

// LogFileOptions - Options passed to `IndexedLogFile` to define index
// writing patterns
type LogFileOptions struct {
	BytesPerNode int64
	Root         string
	TreeDepth    int
}
