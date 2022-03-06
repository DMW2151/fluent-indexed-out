package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	fio "github.com/dmw2151/fluent-indexed-out"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

var defaultOptions = fio.LogFileOptions{
	Root:      `/tmp`,
	TreeDepth: 2,
}

var isIndex = regexp.MustCompile(
	"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-4[a-fA-F0-9]{3}-[8|9|aA|bB][a-fA-F0-9]{3}-[a-fA-F0-9]{12}.idx$",
)

// Message -
type Message struct {
	File      uuid.UUID
	Operation string            `json:"operation"`
	Options   map[string]string `json:"options"`
}

// Response -
type Response struct {
	Content []byte
	Time    time.Time
}

var resolveUnits = map[string]time.Duration{
	"d":  time.Hour * 24,
	"h":  time.Hour,
	"m":  time.Minute,
	"s":  time.Second,
	"ms": time.Millisecond,
}

// HealthCheck - A *VERY* minimal route to handle healthchecks!!
func HealthCheck(w http.ResponseWriter, r *http.Request) {

	log.WithFields(log.Fields{
		"Route":       r.RequestURI,
		"Remote Addr": r.RemoteAddr,
		"User-Agent":  r.Header.Get("USER-AGENT"),
	}).Debug("Handling HealthCheck Request")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(
		Response{
			Content: []byte("OK"),
			Time:    time.Now(),
		},
	)
}

// Query - Handles incoming requests.
func Query(w http.ResponseWriter, r *http.Request) {

	var msg = Message{}

	// Log Request...
	log.WithFields(
		log.Fields{
			"_.timeOpt":   msg.Options["timeOpt"],
			"query.start": msg.Options["start"],
			"query.end":   msg.Options["end"],
			"query.units": msg.Options["units"],
		},
	).Info("Handling Query Request")

	// Read Content...
	b, _ := ioutil.ReadAll(r.Body)
	br := bytes.NewReader(b)

	d := json.NewDecoder(br)
	err := d.Decode(&msg)
	if err != nil {
		log.WithFields(
			log.Fields{
				"error": err.Error(),
				"_.msg": string(b),
			},
		).Error("Failed to Decode Request")
	}

	resp, err := handleQueryOp(&defaultOptions, &msg)

	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(
			&Response{
				Content: []byte(""),
				Time:    time.Now(),
			})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&Response{
		Content: resp,
		Time:    time.Now(),
	})

}

// handleQueryOp
func handleQueryOp(opt *fio.LogFileOptions, m *Message) (b []byte, err error) {

	var t1, t2 int64

	switch timeOpt := m.Options["timeOpt"]; strings.ToLower(timeOpt) {

	// TODO: Debug...
	case "relative":
		startOffset, endOffset, units := m.Options["start"], m.Options["end"], m.Options["units"]
		timeUnits := resolveUnits[units]

		s1, _ := strconv.Atoi(startOffset)
		s2, _ := strconv.Atoi(endOffset)

		t1 = time.Now().Add(-time.Duration(s1) * timeUnits).UnixNano()
		t2 = time.Now().Add(-time.Duration(s2) * timeUnits).UnixNano()

	// TODO: Assert that t0 before t1...
	case "absolute":
		startTimestamp, endTimestamp := m.Options["start"], m.Options["end"]
		tp1, _ := time.Parse(time.RFC3339, startTimestamp)
		tp2, _ := time.Parse(time.RFC3339, endTimestamp)

		t1 = tp1.UnixNano()
		t2 = tp2.UnixNano()

	// Return Error... Invalid Query...
	default:
		return b, err
	}

	err = filepath.Walk(opt.Root, func(path string, info fs.FileInfo, err error) error {

		if isIndex.MatchString(info.Name()) {

			u := uuid.New()

			serIndex, err := fio.ReadSerializedIndex(u, &opt)

			if err != nil {
				fmt.Println(err)
			}

			idxRestored := serIndex.Deserialize(&opt)

			o1 := idx.FirstLTEHead(t1)
			o2 := idx.FirstGTETail(t2)
			b0, _ := idx.OpenBetweenPositions(o1, o2)
		}

		return nil

	})

	return b, err
}
