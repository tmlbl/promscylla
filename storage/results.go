package storage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/prompb"
)

// resultMap encapsulates the map-format result data from the Scylla query and
// compacts it into the unique Prometheus Timeseries that they represent.
type resultMap struct {
	m map[string]*prompb.TimeSeries
}

func newResultMap() *resultMap {
	return &resultMap{
		m: make(map[string]*prompb.TimeSeries),
	}
}

func makeResultKey(result map[string]interface{}) string {
	keys := []string{}
	for k, v := range result {
		if k != "timestamp" && k != "value" {
			keys = append(keys, fmt.Sprintf("%s=%s", k, v))
		}
	}
	sort.Strings(keys)
	return strings.Join(keys, ":")
}

func (r *resultMap) add(result map[string]interface{}) {
	key := makeResultKey(result)
	if _, ok := r.m[key]; !ok {
		ts := &prompb.TimeSeries{}
		ts.Labels = append(ts.Labels, &prompb.Label{
			Name:  "__name__",
			Value: result["metric__name"].(string),
		})
		for k, v := range result {
			if k != "timestamp" && k != "value" &&
				k != "metric__name" && k != "selector" {
				ts.Labels = append(ts.Labels, &prompb.Label{
					Name:  k,
					Value: v.(string),
				})
			}
		}
		r.m[key] = ts
	}

	ts := r.m[key]
	ts.Samples = append(ts.Samples, prompb.Sample{
		Timestamp: result["timestamp"].(int64),
		Value:     result["value"].(float64),
	})

}
