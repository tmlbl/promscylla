package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/tmlbl/promscylla/server"
	"github.com/tmlbl/promscylla/storage"
)

func handleWrite(w http.ResponseWriter, r *http.Request) {
	req, err := server.RemoteWriteRequest(w, r)
	if err != nil {
		return
	}

	for _, ts := range req.Timeseries {
		fmt.Println("Write request for", ts.Labels[0].Value)
		err = store.EnsureSchema(ts)
		if err != nil {
			w.WriteHeader(500)
			log.Println("Error ensuring the schema:", err)
			return
		}
	}
}

func handleRead(w http.ResponseWriter, r *http.Request) {
	req, err := server.RemoteReadRequest(w, r)
	if err != nil {
		return
	}

	fmt.Println(req)
}

var store = &storage.ScyllaStore{}

func main() {
	err := store.Connect([]string{"scylla"})
	if err != nil {
		log.Fatalln(err)
	}
	err = store.Initialize()
	if err != nil {
		log.Fatalln(err)
	}
	http.HandleFunc("/write", handleWrite)
	http.HandleFunc("/read", handleRead)
	http.ListenAndServe(":7337", nil)
}
