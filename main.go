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
	}
}

func handleRead(w http.ResponseWriter, r *http.Request) {
	req, err := server.RemoteReadRequest(w, r)
	if err != nil {
		return
	}

	fmt.Println(req)
}

func main() {
	err := storage.Connect()
	if err != nil {
		log.Fatalln(err)
	}
	http.HandleFunc("/write", handleWrite)
	http.HandleFunc("/read", handleRead)
	http.ListenAndServe(":7337", nil)
}
