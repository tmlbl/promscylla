package storage

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/prometheus/prompb"
	"github.com/scylladb/gocqlx"
)

type ScyllaStore struct {
	sesh     *gocql.Session
	cache    *SchemaCache
	keyspace string
}

func NewScyllaStore(keyspace string) *ScyllaStore {
	return &ScyllaStore{
		cache:    NewSchemaCache(),
		keyspace: keyspace,
	}
}

// Connect initializes the ScyllaDB session
func (s *ScyllaStore) Connect(hosts []string) error {
	cluster := gocql.NewCluster(hosts...)
	cluster.Timeout = time.Second * 10
	sesh, err := cluster.CreateSession()
	if err != nil {
		log.Printf("Failed to connect: %s\n", err)
		time.Sleep(time.Second * 5)
		return s.Connect(hosts)
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
	if name == "" {
		log.Println("Creating keyspace", name)
		err := s.sesh.Query(fmt.Sprintf(`
		create keyspace %s
		WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3}
	`, s.keyspace)).Exec()
		if err != nil {
			return err
		}
	}
	// Bootstrap the schema cache
	columns := []ColumnMeta{}
	log.Println("Populating schema cache...")
	err := gocqlx.Query(s.sesh.Query(`
		select keyspace_name,table_name,column_name from system_schema.columns
		where keyspace_name = ?
	`, s.keyspace), []string{"keyspace_name", "table_name", "column_name"}).Iter().Select(&columns)
	if err != nil {
		return err
	}
	log.Println("Adding", len(columns), "columns to cache")
	for _, col := range columns {
		go s.cache.AddColumn(col)
	}
	return nil
}

func tsLabelMap(ts *prompb.TimeSeries) map[string]string {
	m := make(map[string]string)
	for _, l := range ts.Labels {
		m[l.Name] = l.Value
	}
	return m
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

func (s *ScyllaStore) updateCache(tableName string) error {
	log.Println("Updating cache for", tableName)
	columns, err := s.getColumns(tableName)
	if err != nil {
		return err
	}
	for _, col := range columns {
		s.cache.AddColumn(col)
	}
	return nil
}

func (s *ScyllaStore) EnsureSchema(ts *prompb.TimeSeries) error {
	name := getTableName(ts)
	if s.cache.Satisfies(NewSchema(ts)) {
		log.Printf("Schema for %s satisfied by cache\n", name)
		return nil
	}
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
			metric__name ASCII,
			selector ASCII,
			value DOUBLE,
			timestamp BIGINT,
			%s,
			PRIMARY KEY (metric__name, selector, timestamp)
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

	return s.updateCache(name)
}
func getColumnNames(ts *prompb.TimeSeries) []string {
	cols := []string{}
	for _, label := range ts.Labels {
		if label.Name != "__name__" {
			cols = append(cols, label.Name)
		}
	}
	return cols
}

func getLabelValues(ts *prompb.TimeSeries) []string {
	vals := []string{}
	for _, label := range ts.Labels {
		if label.Name != "__name__" {
			vals = append(vals, fmt.Sprintf("'%s'", label.Value))
		}
	}
	return vals
}

func (s *ScyllaStore) WriteSamples(ts *prompb.TimeSeries) error {
	tableName := getTableName(ts)
	selector := makeSelector(ts)
	insertTemplate := fmt.Sprintf("INSERT INTO %s.%s (metric__name, selector, timestamp, %s, value) VALUES ('%s', '%s', ?, %s, ?)",
		s.keyspace, tableName, strings.Join(getColumnNames(ts), ", "),
		ts.Labels[0].Value, selector, strings.Join(getLabelValues(ts), ", "))

	//	fmt.Println(insertTemplate)

	batch := gocql.NewBatch(gocql.LoggedBatch)
	for _, sample := range ts.Samples {
		batch.Query(insertTemplate, sample.Timestamp, sample.Value)
	}

	log.Println("Writing", len(ts.Samples), "samples to", tableName)

	return s.sesh.ExecuteBatch(batch)
}

func (s *ScyllaStore) ReadSamples(query *prompb.Query) (*prompb.TimeSeries, error) {
	return nil, nil
}
