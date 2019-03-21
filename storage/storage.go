package storage

import (
	"github.com/gocql/gocql"
)

var sesh *gocql.Session

// Connect initializes the ScyllaDB session
func Connect() error {
	cluster := gocql.NewCluster("scylla")
	s, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	sesh = s
	return nil
}
