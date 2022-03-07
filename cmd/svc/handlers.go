package main

import (
	"bufio"
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
	"sync"
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

var hitIndexFunc = func(fn string, t1 int64, t2 int64, rStg *StagedResponse) {

	serIndex, err := fio.ReadSerializedIndex(
		fmt.Sprintf(`%s/%s`, defaultOptions.Root, fn),
	)

	if err != nil {
		fmt.Println(err)
	}

	idx := serIndex.Deserialize(&defaultOptions)

	o1, o2 := idx.FirstLTEHead(t1), idx.FirstGTETail(t2)
	bPartial, _ := idx.OpenBetweenPositions(o1, o2)

	// Turn Bytes -> Records
	rdr := bytes.NewReader(bPartial)
	sc := bufio.NewScanner(rdr)

	// Probably can't check length of response and make array (??)
	var contents = []fio.Record{}

	// Filter to exact location
	for sc.Scan() {
		r := fio.Record{}
		json.Unmarshal(sc.Bytes(), &r)
		if (t1 < r.Timestamp.UnixNano()) && (r.Timestamp.UnixNano() < t2) {
			contents = append(contents, r)
		}
	}

	// Lock so only one goroutine at a time can access the map c.v.
	rStg.mu.Lock()
	rStg.Contents = append(rStg.Contents, contents...)
	defer rStg.mu.Unlock()
}

// StagedResponse -
type StagedResponse struct {
	Contents []fio.Record
	mu       sync.Mutex
}

// Message -
type Message struct {
	File      uuid.UUID
	Operation string            `json:"operation"`
	Options   map[string]string `json:"options"`
}

// Response -
type Response struct {
	Status string
	Time   time.Time
	Body   []fio.Record
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
			Status: "OK",
			Time:   time.Now(),
		},
	)
}

// Query - Handles incoming requests.
func Query(w http.ResponseWriter, r *http.Request) {

	var msg = Message{}

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

	// Log Request...
	log.WithFields(
		log.Fields{
			"_.timeOpt":   msg.Options["timeOpt"],
			"query.start": msg.Options["start"],
			"query.end":   msg.Options["end"],
			"query.units": msg.Options["units"],
		},
	).Info("Handling Query Request")

	resp, err := handleQueryOp(&msg)

	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(
			&Response{
				Status: "",
				Time:   time.Now(),
			})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(&Response{
		Status: "OK",
		Time:   time.Now(),
		Body:   resp,
	})
}

// handleQueryOp
func handleQueryOp(m *Message) (resp []fio.Record, err error) {

	var (
		t1, t2 int64
		stgR   StagedResponse
		wg     sync.WaitGroup
	)

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

		t1, t2 = tp1.UnixNano(), tp2.UnixNano()

	// Return Error... Invalid Query...
	default:
		return resp, err
	}

	err = filepath.Walk(defaultOptions.Root, func(path string, info fs.FileInfo, err error) error {

		if info != nil {
			if isIndex.MatchString(info.Name()) {
				wg.Add(1)

				go func() {
					defer wg.Done()
					hitIndexFunc(info.Name(), t1, t2, &stgR)
				}()
			}
		}
		return nil
	})

	wg.Wait()

	// Marshall into Records...
	return stgR.Contents, err
}
