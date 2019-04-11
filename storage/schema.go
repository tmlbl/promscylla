package storage

import (
	"fmt"
	"strings"

	"github.com/prometheus/prometheus/prompb"
)

// Schema is a struct representation of Scylla schema information
// extracted from a Prometheus TimeSeries
type Schema struct {
	TableName  string
	LabelNames map[string]bool
}

// NewSchema extracts schema information from a prompb.TimeSeries
func NewSchema(ts *prompb.TimeSeries) *Schema {
	return &Schema{
		TableName:  getTimeSeriesTableName(ts),
		LabelNames: getLabelNames(ts),
	}
}

// Satisfies returns whether the schema information in the receiver is
// a superset of the schema information in the parameter
func (s *Schema) Satisfies(s2 *Schema) bool {
	if s.TableName != s2.TableName {
		return false
	}
	for label, _ := range s2.LabelNames {
		if _, ok := s.LabelNames[label]; !ok {
			return false
		}
	}
	return true
}

// Get the name of the Scylla table that should be used
// We take the first 1 or 2 underscore-separated words from the
// metric name. This gives a good distribution of the metrics without
// having to create too many tables.
func getTableName(metricName string) string {
	parts := strings.Split(metricName, "_")
	name := parts[0]
	if len(parts) > 1 {
		name = fmt.Sprintf("%s_%s", name, parts[1])
	}
	return name
}

func getTimeSeriesTableName(ts *prompb.TimeSeries) string {
	return getTableName(ts.Labels[0].Value)
}

func getLabelNames(ts *prompb.TimeSeries) map[string]bool {
	names := map[string]bool{}
	for _, label := range ts.Labels {
		if label.Name != "__name__" {
			names[label.Name] = true
		}
	}
	return names
}

// Selector is an ASCII string generated from all labels and their values for a TimeSeries
// This is the definition of a unique series in Prometheus.
func makeSelector(ts *prompb.TimeSeries) string {
	s := []string{}
	for _, label := range ts.Labels[1:] {
		s = append(s, fmt.Sprintf("%s=%s", label.Name, label.Value))
	}
	return strings.Join(s, ":")
}
