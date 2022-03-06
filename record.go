package indexedlogplugin

import "time"

// Record -
type Record struct {
	Timestamp time.Time   `json:"Timestamp"`
	Tag       string      `json:"Tag"`
	Key       interface{} `json:"Key"`
	Value     interface{} `json:"Value"`
}
