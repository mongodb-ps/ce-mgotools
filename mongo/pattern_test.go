package mongo

import (
	"reflect"
	"testing"
)

func TestPattern_NewPattern(t *testing.T) {
	s := []map[string]interface{}{
		{"a": 5},
		{"a": 5, "b": "y"},
		{"a": Object{"$in": "y"}},
		{"a": Object{"$gt": 5}},
	}
	d := []map[string]interface{}{
		{"a": V{}},
		{"a": V{}, "b": V{}},
		{"a": Object{"$in": V{}}},
		{"a": Object{"$gt": V{}}},
	}
	for i := range s {
		if p, err := NewPattern(s[i]); err != nil {
			t.Errorf("pattern error at %d: %s", i, err)
		} else if !reflect.DeepEqual(p.Pattern, d[i]) {
			t.Errorf("pattern mismatch at %d: %#v %#v", i, s[i], d[i])
		}
	}
}
