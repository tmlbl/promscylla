package server

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/prompb"

	"github.com/golang/snappy"
)

func RemoteWrite(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, ts := range req.Timeseries {
		fmt.Println("Writing series with labels", ts.Labels)
	}
}

func RemoteRead(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, query := range req.Queries {
		for _, matcher := range query.Matchers {
			fmt.Println("MATCHER", matcher.Name)
		}
	}
}

func Serve() {
	http.HandleFunc("/write", RemoteWrite)
	http.HandleFunc("/read", RemoteRead)
	http.ListenAndServe(":7337", nil)
}
