package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/prometheus/prompb"
	"github.com/scylladb/gocqlx"
)

type ScyllaStore struct {
	sesh     *gocql.Session
	keyspace string
}

// Connect initializes the ScyllaDB session
func (s *ScyllaStore) Connect(hosts []string) error {
	cluster := gocql.NewCluster(hosts...)
	cluster.Timeout = time.Second * 10
	sesh, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	s.sesh = sesh
	return nil
}

func (s *ScyllaStore) Initialize() error {
	// Make sure the keyspace exists
	var name string
	s.sesh.Query(`
		SELECT keyspace_name FROM system_schema.keyspaces
		WHERE keyspace_name = ?
	`, s.keyspace).Scan(&name)
	if name != "" {
		return nil
	}
	return s.sesh.Query(fmt.Sprintf(`
		create keyspace %s
		WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}
	`, s.keyspace)).Exec()
}

func tsLabelMap(ts *prompb.TimeSeries) map[string]string {
	m := make(map[string]string)
	for _, l := range ts.Labels {
		m[l.Name] = l.Value
	}
	return m
}

type ColumnMeta struct {
	KeyspaceName string
	TableName    string
	ColumnName   string
}

func (s *ScyllaStore) getColumns(tableName string) ([]ColumnMeta, error) {
	columns := []ColumnMeta{}
	err := gocqlx.Query(s.sesh.Query(`
		select keyspace_name,table_name,column_name from system_schema.columns
		where keyspace_name = ? and table_name = ?`,
		s.keyspace, tableName), []string{"keyspace_name", "table_name", "column_name"}).Iter().Select(&columns)
	if err != nil {
		return nil, err
	}
	return columns, nil
}

func (s *ScyllaStore) EnsureSchema(ts *prompb.TimeSeries) error {
	name := strings.Split(ts.Labels[0].Value, "_")[0]
	columns, err := s.getColumns(name)
	if err != nil {
		return err
	}

	// If there are no columns at all, we need to create the table
	if len(columns) == 0 {
		fmt.Println("Creating the table for", name)
		columnDefs := []string{}
		for _, label := range ts.Labels {
			if label.Name != "__name__" {
				columnDefs = append(columnDefs, fmt.Sprintf("%s ASCII", label.Name))
			}
		}
		stmt := fmt.Sprintf(`CREATE TABLE %s.%s (
			name ASCII,
			selector ASCII,
			value DOUBLE,
			timestamp BIGINT,
			%s,
			PRIMARY KEY (name, selector, timestamp)
		) WITH CLUSTERING ORDER BY (selector DESC, timestamp ASC)`, s.keyspace, name, strings.Join(columnDefs, ", "))
		fmt.Println(stmt)
		err = s.sesh.Query(stmt).Exec()
		if err != nil {
			return err
		}
	} else {
		// Make sure all labels can be mapped to columns
		var present = map[string]bool{}
		for _, label := range ts.Labels {
			present[label.Name] = false
		}
		for _, col := range columns {
			if _, ok := present[col.ColumnName]; ok {
				present[col.ColumnName] = true
			}
		}
		// Build the statement to add any missing columns
		missing := []string{}
		for label, there := range present {
			if !there && label != "__name__" {
				missing = append(missing, label)
			}
		}

		if len(missing) > 0 {
			for i := range missing {
				stmt := fmt.Sprintf("ALTER TABLE %s.%s ADD %s ASCII", s.keyspace, name, missing[i])
				fmt.Println(stmt)
				err = s.sesh.Query(stmt).Exec()
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func makeSelector(ts *prompb.TimeSeries) string {
	s := []string{}
	for _, label := range ts.Labels {
		s = append(s, fmt.Sprintf("%s=%s", label.Name, label.Value))
	}
	return strings.Join(s, ":")
}

func (s *ScyllaStore) WriteSamples(ts *prompb.TimeSeries) error {
	// template := fmt.Sprintf("INSERT INTO metrics.%s ", )
	// selector := makeSelector(ts)
	// batch := s.sesh.NewBatch(gocql.UnloggedBatch)
	// for _, s := range ts.Samples {
	// stmt := fmt.Sprintf("INSERT INTO metrics.%s ")
	// }
	return nil
}
