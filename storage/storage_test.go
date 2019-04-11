package storage

import (
	"fmt"
	"log"
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func getTestStore(name string) *ScyllaStore {
	store := ScyllaStore{
		keyspace: name,
		cache:    NewSchemaCache(),
	}

	err := store.Connect([]string{"localhost"})
	if err != nil {
		log.Fatalln(err)
	}

	store.sesh.Query(fmt.Sprintf("DROP KEYSPACE %s", name)).Exec()

	err = store.Initialize()
	if err != nil {
		log.Fatalln(err)
	}

	return &store
}

func TestEnsureSchema(t *testing.T) {
	store := getTestStore("ensure_schema_test")

	ts := prompb.TimeSeries{}
	ts.Labels = []*prompb.Label{
		&prompb.Label{
			Name:  "__name__",
			Value: "test_metric_1",
		},
		&prompb.Label{
			Name:  "foo",
			Value: "bar",
		},
	}
	err := store.EnsureSchema(&ts)
	if err != nil {
		t.Error(err)
	}

	// Make sure the columns were created
	columns, _ := store.getColumns("test_metric")
	if len(columns) != 5 {
		t.Errorf("Expected %d columns, got %d", 5, len(columns))
	}

	// Add another column
	ts.Labels = append(ts.Labels, &prompb.Label{
		Name:  "job",
		Value: "testing",
	})
	err = store.EnsureSchema(&ts)
	if err != nil {
		t.Error(err)
	}
	columns, _ = store.getColumns("test_metric")
	if len(columns) != 6 {
		t.Errorf("Expected %d columns, got %d", 6, len(columns))
	}
}

func TestReadWriteSamples(t *testing.T) {
	store := getTestStore("read_write_test")

	ts := prompb.TimeSeries{}
	ts.Labels = []*prompb.Label{
		&prompb.Label{
			Name:  "__name__",
			Value: "test_metric_1",
		},
		&prompb.Label{
			Name:  "foo",
			Value: "bar",
		},
	}
	ts.Samples = []prompb.Sample{
		prompb.Sample{
			Timestamp: 200,
		},
	}
	store.EnsureSchema(&ts)
	store.WriteSamples(&ts)
	series, err := store.ReadSamples(&prompb.Query{
		StartTimestampMs: 100,
		EndTimestampMs:   300,
		Matchers: []*prompb.LabelMatcher{
			&prompb.LabelMatcher{
				Name:  "__name__",
				Value: "test_metric_1",
			},
		},
	})
	if err != nil {
		t.Error(err)
	}
	if len(series.Samples) != 1 {
		t.Errorf("Expected 1 sample to be returned")
	}
}
