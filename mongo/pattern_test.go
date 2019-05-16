package mongo

import (
	"reflect"
	"testing"
)

type (
	O = map[string]interface{}
	A = []interface{}
)

func TestPattern_NewPattern(t *testing.T) {
	s := []O{
		{"a": 5},
		{"a": 5, "b": "y"},
		{"a": O{"$in": "y"}},
		{"a": O{"$gt": 5}},
		{"a": O{"$exists": true}},
		{"a.b": "y"},
		{"$or": A{O{"a": 5}, O{"b": 5}}}, // Logical operator without compression.
		{"$or": A{O{"b": 5}, O{"a": 5}}}, // Sort of keys in logical operator.
		{"$or": A{O{"b": 5, "a": 5}, O{"a": 5}}},
		{"$and": A{O{"$or": A{O{"a": 5}, O{"b": 5}}}, O{"$or": A{O{"c": 5}, O{"d": 5}}}}},
		{"_id": ObjectId{}},
		{"a": O{"$in": A{5, 5, 5}}},
		{"a": O{"$elemMatch": O{"b": 5, "c": O{"$gte": 5}}}},
		{"a": O{"$geoWithin": O{"$center": A{A{5, 5}, 5}}}},
		{"a": O{"$geoWithin": O{"$geometry": O{"a": "y", "b": A{5, 5}}}}},
	}
	d := []O{
		{"a": V{}},
		{"a": V{}, "b": V{}},
		{"a": V{}},
		{"a": V{}},
		{"a": V{}},
		{"a.b": V{}},
		{"$or": A{O{"a": V{}}, O{"b": V{}}}}, // Logical operator without compression.
		{"$or": A{O{"a": V{}}, O{"b": V{}}}}, // Sort of keys in logical operator.
		{"$or": A{O{"a": V{}}, O{"a": V{}, "b": V{}}}},
		{"$and": A{O{"$or": A{O{"a": V{}}, O{"b": V{}}}}, O{"$or": A{O{"c": V{}}, O{"d": V{}}}}}},
		{"_id": V{}},
		{"a": V{}},
		{"a": O{"$elemMatch": O{"b": V{}, "c": V{}}}},
		{"a": O{"$geoWithin": O{"$center": V{}}}},
		{"a": O{"$geoWithin": O{"$geometry": O{"a": V{}, "b": V{}}}}},
	}
	if len(s) != len(d) {
		t.Fatalf("mismatch between array sizes, %d and %d", len(s), len(d))
	}

	for i := range s {
		if p := NewPattern(s[i]); !deepEqual(p.pattern, d[i]) {
			t.Errorf("pattern mismatch at %d:\n\t\t%#v\n\t\t%#v", i+1, d[i], s[i])
		}
	}
}

func TestPattern_Equals(t *testing.T) {
	s := []O{
		{},
		{"a": V{}},
		{"a": V{}, "b": V{}},
		{"a": O{"b": V{}}},
		{"a": A{}},
		{"a": A{V{}, V{}}},
		{"a": O{}},
		{"a": A{O{"a": V{}}}},
		{"a": V{}},
	}
	d := []O{
		{"a": V{}},
		{"b": V{}},
		{"b": V{}, "a": V{}, "c": V{}},
		{"a": O{"c": V{}}},
		{"a": A{V{}}},
		{"a": A{V{}}},
		{"a": A{}},
		{"a": A{O{"b": V{}}}},
		{"a": O{}},
	}

	if len(s) != len(d) {
		t.Fatalf("mismatch between array sizes, %d and %d", len(s), len(d))
	}

	for i := range s {
		p := Pattern{s[i], true}
		if !p.Equals(Pattern{s[i], true}) {
			t.Errorf("equality mismatch at %d: %#v", i, s[i])
		}
	}
	for i := range s {
		p := Pattern{s[i], true}
		r := Pattern{d[i], true}
		if p.Equals(r) {
			t.Errorf("equality match at %d:\n%#v\n%v", i, s[i], d[i])
		}
	}
}
func TestPattern_IsEmpty(t *testing.T) {
	p := Pattern{}
	if !p.IsEmpty() {
		t.Errorf("unexpected initialized variable")
	}
	r := NewPattern(O{})
	if r.IsEmpty() {
		t.Errorf("unexpected uninitialzied value")
	}
}
func TestPattern_Pattern(t *testing.T) {
	p := Pattern{}
	if p.Pattern() != nil {
		t.Errorf("pattern should be empty")
	}
}
func TestPattern_String(t *testing.T) {
	s := []Pattern{
		{O{"a": V{}}, true},
		{O{"a": V{}, "b": V{}}, true},
		{O{"a": A{V{}, V{}}}, true},
		{O{}, true},
		{O{"a": A{}}, true},
		{O{"a": A{O{"b": V{}}}}, true},
		{O{"a": A{A{V{}}}}, true},
	}
	d := []string{
		`{ "a": 1 }`,
		`{ "a": 1, "b": 1 }`,
		`{ "a": 1 }`,
		`{}`,
		`{ "a": 1 }`,
		`{ "a": [ { "b": 1 } ] }`,
		`{ "a": 1 }`,
	}
	if len(s) != len(d) {
		t.Fatalf("mismatch between array sizes, %d and %d", len(s), len(d))
	}
	for i := range s {
		if s[i].String() != d[i] {
			t.Errorf("pattern mismatch (%d), expected '%s', got '%s'", i, d[i], s[i].String())
		}
	}
}

func TestPattern_mtools(t *testing.T) {
	oid1, _ := NewObjectId("1234564863acd10e5cbf5f6e")
	oid2, _ := NewObjectId("1234564863acd10e5cbf5f7e")
	oid3, _ := NewObjectId("528556616dde23324f233168")

	s := []O{
		//`{"d": {"$gt": 2, "$lt": 4}, "b": {"$gte": 3}, "c": {"$nin": [1, "foo", "bar"]}, "$or": [{"a":1}, {"b":1}] }`,
		{
			"d": O{"$gt": 2, "$lt": 4}, "b": O{"$gte": 3}, "c": O{"$nin": A{1, "foo", "bar"}}, "$or": A{O{"a": 1}, O{"b": 1}},
		},

		//`{"a": {"$gt": 2, "$lt": 4}, "b": {"$nin": [1, 2, 3]}, "$or": [{"a":1}, {"b":1}] }`,
		{
			"a": O{"$gt": 2, "$lt": 4}, "b": O{"$nin": A{1, 2, 3}}, "$or": A{O{"a": 1}, O{"b": 1}},
		},

		//`{"a": {"$gt": 2, "$lt": 4}, "b": {"$in": [ ObjectId("1234564863acd10e5cbf5f6e"), ObjectId("1234564863acd10e5cbf5f7e") ] } }`,
		{
			"a": O{"$gt": 2, "$lt": 4}, "b": O{"$in": A{oid1, oid2}},
		},

		//`{ "sk": -1182239108, "_id": { "$in": [ ObjectId("1234564863acd10e5cbf5f6e"), ObjectId("1234564863acd10e5cbf5f7e") ] } }`,
		{
			"sk": -1182239108, "_id": O{"$in": A{oid1, oid1}},
		},

		//`{ "a": 1, "b": { "c": 2, "d": "text" }, "e": "more test" }`,
		{
			"a": 1, "b": O{"c": 2, "d": "text"}, "e": "more test",
		},

		//`{ "_id": ObjectId("528556616dde23324f233168"), "config": { "_id": 2, "host": "localhost:27017" }, "ns": "local.oplog.rs" }`,
		{
			"_id": oid3, "config": O{"_id": 2, "host": "localhost:27017"}, "ns": "local.oplog.rs",
		},
	}

	d := []string{
		`{"$or": [{"a": 1}, {"b": 1}], "b": 1, "c": {"$nin": 1}, "d": 1}`,
		`{"$or": [{"a": 1}, {"b": 1}], "a": 1, "b": {"$nin": 1}}`,
		`{"a": 1, "b": 1}`,
		`{"_id": 1, "sk": 1}`,
		`{"a": 1, "b": {"c": 1, "d": 1}, "e": 1}`,
		`{"_id": 1, "config": {"_id": 1, "host": 1}, "ns": 1}`,
	}

	for i, m := range s {
		p := NewPattern(m)

		if p.StringCompact() != d[i] {
			t.Errorf("mismatch at %d, got '%s', expected '%s'", i, p.StringCompact(), d[i])
		}
	}
}

var patterns = []Pattern{
	NewPattern(O{}),
	NewPattern(O{"a": 1}),
	NewPattern(O{"a": 1, "b": 1}),
	NewPattern(O{"a": O{"b": 1}}),
	NewPattern(O{"a": O{"b": A{1, 1}}}),
	NewPattern(O{"a": O{"b": O{"c": O{"d": 1}}}}),
	NewPattern(O{"a": A{O{"a": 1}, O{"b": 1}, O{"c": 1}}}),
	NewPattern(O{"a": 1, "b": 1, "c": 1, "d": 1}),
	NewPattern(O{"a": A{O{"b": 1}}, "c": A{O{"d": 1}}}),
}

func BenchmarkPattern_Equals(b *testing.B) {
	for i := 0; i < b.N; i += 1 {
		for _, s := range patterns {
			s.Equals(s)
		}
	}
}
func BenchmarkPattern_String(b *testing.B) {
	for i := 0; i < b.N; i += 1 {
		for _, s := range patterns {
			s.String()
		}
	}
}
func BenchmarkPattern_MatchEquals(b *testing.B) {
	match := NewPattern(O{"a": A{O{"a": 1}, O{"b": 1}, O{"c": 1}}})
	size := len(patterns)
	for i := 0; i < b.N; i += 1 {
		for j := 0; j < size; j += 1 {
			if patterns[j].Equals(match) {
			}
		}
	}
}
func BenchmarkPattern_MatchString(b *testing.B) {
	match := NewPattern(O{"a": A{O{"a": 1}, O{"b": 1}, O{"c": 1}}})
	var strings = make([]string, len(patterns))
	size := len(patterns)
	for i := 0; i < size; i += 1 {
		strings[i] = patterns[i].String()
	}
	for i := 0; i < b.N; i += 1 {
		s := match.String()
		for j := 0; j < size; j += 1 {
			if s == strings[j] {
			}
		}
	}
}
func BenchmarkReflection_DeepEqual(b *testing.B) {
	for i := 0; i < b.N; i += 1 {
		for _, s := range patterns {
			reflect.DeepEqual(s, s)
		}
	}
}
