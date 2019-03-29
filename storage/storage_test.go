package storage

import (
	"testing"

	"github.com/prometheus/prometheus/prompb"
)

func TestEnsureSchema(t *testing.T) {
	store := ScyllaStore{
		keyspace: "test1",
	}
	err := store.Connect([]string{"localhost"})
	if err != nil {
		t.Error(err)
	}
	// Clear out test database
	store.sesh.Query("DROP KEYSPACE test1").Exec()

	err = store.Initialize()
	if err != nil {
		t.Error(err)
	}

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
	err = store.EnsureSchema(&ts)
	if err != nil {
		t.Error(err)
	}

	// Make sure the columns were created
	columns, _ := store.getColumns("test")
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
	columns, _ = store.getColumns("test")
	if len(columns) != 6 {
		t.Errorf("Expected %d columns, got %d", 6, len(columns))
	}
}
