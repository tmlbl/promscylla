package storage

import (
	"testing"
)

func TestSchemaSatisfies(t *testing.T) {
	for _, tst := range []struct {
		s1            Schema
		s2            Schema
		shouldSatisfy bool
	}{
		{
			s1: Schema{
				TableName: "foo_bar",
				LabelNames: map[string]bool{
					"baz": true,
				},
			},
			s2: Schema{
				TableName: "foo_bar",
				LabelNames: map[string]bool{
					"baz": true,
				},
			},
			shouldSatisfy: true,
		},
		{
			s1: Schema{
				TableName: "foo_bar",
				LabelNames: map[string]bool{
					"baz": true,
				},
			},
			s2: Schema{
				TableName: "foo_bar",
				LabelNames: map[string]bool{
					"baz": true,
					"bix": true,
				},
			},
			shouldSatisfy: false,
		},
	} {
		if (tst.s1.Satisfies(&tst.s2)) != tst.shouldSatisfy {
			t.Errorf("s1 satisfies s2: wrong result")
		}
	}
}
