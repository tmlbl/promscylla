package storage

import (
	"log"
	"sync"
)

// SchemaCache stores information about the table schemas that have already
// been created in memory so that extra queries to the system_schema table are
// not required.
type SchemaCache struct {
	mu    sync.Mutex
	cache map[string]*Schema // key is table name
}

func NewSchemaCache() *SchemaCache {
	return &SchemaCache{
		cache: map[string]*Schema{},
	}
}

// AddColumn adds a column read from the system_schema table to the cache
func (sc *SchemaCache) AddColumn(c ColumnMeta) {
	sc.mu.Lock()
	_, ok := sc.cache[c.TableName]
	if !ok {
		log.Println("Adding", c.TableName, "to cache")
		sc.cache[c.TableName] = &Schema{
			TableName:  c.TableName,
			LabelNames: map[string]bool{},
		}
	}
	schema := sc.cache[c.TableName]
	schema.LabelNames[c.ColumnName] = true
	sc.mu.Unlock()
}

func (sc *SchemaCache) Satisfies(s *Schema) bool {
	sc.mu.Lock()
	schema := sc.cache[s.TableName]
	defer sc.mu.Unlock()
	return schema.Satisfies(s)
}
