package main

import (
	"log"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/tmlbl/promscylla/server"
	"github.com/tmlbl/promscylla/storage"
)

func handleWrite(w http.ResponseWriter, r *http.Request) {
	req, err := server.RemoteWriteRequest(w, r)
	if err != nil {
		return
	}

	for _, ts := range req.Timeseries {
		//		fmt.Println("Write request for", ts.Labels[0].Value)
		err = store.EnsureSchema(ts)
		if err != nil {
			w.WriteHeader(500)
			log.Println("Error ensuring the schema:", err)
			return
		}
		err = store.WriteSamples(ts)
		if err != nil {
			w.WriteHeader(500)
			log.Println("Error writing the samples:", err)
			return
		}
	}
}

func handleRead(w http.ResponseWriter, r *http.Request) {
	req, err := server.RemoteReadRequest(w, r)
	if err != nil {
		return
	}
	resp := prompb.ReadResponse{}
	for _, q := range req.Queries {
		series, err := store.ReadSamples(q)
		if err != nil {
			w.WriteHeader(500)
			log.Println("Error reading samples:", err)
			return
		}
		resp.Results = append(resp.Results, &prompb.QueryResult{
			Timeseries: series,
		})
	}
	data, err := proto.Marshal(&resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		log.Println("Error writing response", err)
	}
}

var store *storage.ScyllaStore

func main() {
	store = storage.NewScyllaStore("metrics")
	err := store.Connect([]string{"scylladb-node1", "scylladb-node2", "scylladb-node3"})
	if err != nil {
		log.Fatalln(err)
	}
	err = store.Initialize()
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Starting the web server")
	http.HandleFunc("/write", handleWrite)
	http.HandleFunc("/read", handleRead)
	http.ListenAndServe(":7337", nil)
}
