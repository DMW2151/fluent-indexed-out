package main

import (
	"net/http"
	"os"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

func init() {

	// Set logging config conditional on the environment - Always to STDOUT
	// and always with a specific time & msg format...
	log.SetOutput(os.Stdout)

	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05.0000",
	})

	log.SetLevel(log.DebugLevel)
	log.SetReportCaller(false)

	// Log Level Cfg
	log.WithFields(log.Fields{
		"Level":         log.DebugLevel,
		"Report Caller": false,
	}).Info("Set Logging Level")

}

func main() {

	// Init router and define routes for the API
	router := mux.NewRouter().StrictSlash(true)

	// HealthCheck - For determining if the container is healthy or not...
	router.Path("/health/").
		HandlerFunc(HealthCheck).
		Methods("GET")

	router.Path("/index/").
		HandlerFunc(Query).
		Methods("GET")

	// Start service w.o a graceful shutdown. Barbaric serving pattern...
	// See mux docs: https://github.com/gorilla/mux#graceful-shutdown
	srv := &http.Server{
		Addr:    "0.0.0.0:2151",
		Handler: router,
	}

	if err := srv.ListenAndServe(); err != nil {
		log.WithFields(log.Fields{"Error": err}).Error("API Exited")
	}

}
