package mongo

import (
	"reflect"
	"testing"
)

func TestPattern_NewPattern(t *testing.T) {
	s := []Object{
		{"a": 5},
		{"a": 5, "b": "y"},
		{"a": Object{"$in": "y"}},
		{"a": Object{"$gt": 5}},
		{"a": Object{"$exists": true}},
		{"a.b": "y"},
		{"$or": Array{Object{"a": 5}, Object{"b": 5}}},
		{"$and": Array{Object{"$or": Array{Object{"a": 5}, Object{"b": 5}}}, Object{"$or": Array{Object{"c": 5}, Object{"d": 5}}}}},
		{"_id": ObjectId{}},
		{"a": Object{"$in": Array{5, 5, 5}}},
		{"a": Object{"$elemMatch": Object{"b": 5, "c": Object{"$gte": 5}}}},
		{"a": Object{"$geoWithin": Object{"$center": Array{Array{5, 5}, 5}}}},
		{"a": Object{"$geoWithin": Object{"$geometry": Object{"a": "y", "b": Array{5, 5}}}}},
	}
	d := []Object{
		{"a": V{}},
		{"a": V{}, "b": V{}},
		{"a": V{}},
		{"a": V{}},
		{"a": V{}},
		{"a.b": V{}},
		{"$or": Array{Object{"a": V{}}, Object{"b": V{}}}},
		{"$and": Array{Object{"$or": Array{Object{"a": V{}}, Object{"b": V{}}}}, Object{"$or": Array{Object{"c": V{}}, Object{"d": V{}}}}}},
		{"_id": V{}},
		{"a": V{}},
		{"a": Object{"$elemMatch": Object{"b": V{}, "c": V{}}}},
		{"a": Object{"$geoWithin": Object{"$center": V{}}}},
		{"a": Object{"$geoWithin": Object{"$geometry": Object{"a": V{}, "b": V{}}}}},
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
	s := []Object{
		{},
		{"a": V{}},
		{"a": V{}, "b": V{}},
		{"a": Object{"b": V{}}},
		{"a": Array{}},
		{"a": Array{V{}, V{}}},
		{"a": Object{}},
		{"a": Array{Object{"a": V{}}}},
		{"a": V{}},
	}
	d := []Object{
		{"a": V{}},
		{"b": V{}},
		{"b": V{}, "a": V{}, "c": V{}},
		{"a": Object{"c": V{}}},
		{"a": Array{V{}}},
		{"a": Array{V{}}},
		{"a": Array{}},
		{"a": Array{Object{"b": V{}}}},
		{"a": Object{}},
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
	r := NewPattern(Object{})
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
		{Object{"a": V{}}, true},
		{Object{"a": V{}, "b": V{}}, true},
		{Object{"a": Array{V{}, V{}}}, true},
		{Object{}, true},
		{Object{"a": Array{}}, true},
		{Object{"a": Array{Object{"b": V{}}}}, true},
		{Object{"a": Array{Array{V{}}}}, true},
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
	s := []Object{
		//`{"d": {"$gt": 2, "$lt": 4}, "b": {"$gte": 3}, "c": {"$nin": [1, "foo", "bar"]}, "$or": [{"a":1}, {"b":1}] }`,
		{
			"d": Object{"$gt": 2, "$lt": 4}, "b": Object{"$gte": 3}, "c": Object{"$nin": Array{1, "foo", "bar"}}, "$or": Array{Object{"a": 1}, Object{"b": 1}},
		},

		//`{"a": {"$gt": 2, "$lt": 4}, "b": {"$nin": [1, 2, 3]}, "$or": [{"a":1}, {"b":1}] }`,
		{
			"a": Object{"$gt": 2, "$lt": 4}, "b": Object{"$nin": Array{1, 2, 3}}, "$or": Array{Object{"a": 1}, Object{"b": 1}},
		},

		//`{"a": {"$gt": 2, "$lt": 4}, "b": {"$in": [ ObjectId("1234564863acd10e5cbf5f6e"), ObjectId("1234564863acd10e5cbf5f7e") ] } }`,
		{
			"a": Object{"$gt": 2, "$lt": 4}, "b": Object{"$in": Array{ObjectId("1234564863acd10e5cbf5f6e"), ObjectId("1234564863acd10e5cbf5f7e")}},
		},

		//`{ "sk": -1182239108, "_id": { "$in": [ ObjectId("1234564863acd10e5cbf5f6e"), ObjectId("1234564863acd10e5cbf5f7e") ] } }`,
		{
			"sk": -1182239108, "_id": Object{"$in": Array{ObjectId("1234564863acd10e5cbf5f6e"), ObjectId("1234564863acd10e5cbf5f6e")}},
		},

		//`{ "a": 1, "b": { "c": 2, "d": "text" }, "e": "more test" }`,
		{
			"a": 1, "b": Object{"c": 2, "d": "text"}, "e": "more test",
		},

		//`{ "_id": ObjectId("528556616dde23324f233168"), "config": { "_id": 2, "host": "localhost:27017" }, "ns": "local.oplog.rs" }`,
		{
			"_id": ObjectId("528556616dde23324f233168"), "config": Object{"_id": 2, "host": "localhost:27017"}, "ns": "local.oplog.rs",
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
	NewPattern(Object{}),
	NewPattern(Object{"a": 1}),
	NewPattern(Object{"a": 1, "b": 1}),
	NewPattern(Object{"a": Object{"b": 1}}),
	NewPattern(Object{"a": Object{"b": Array{1, 1}}}),
	NewPattern(Object{"a": Object{"b": Object{"c": Object{"d": 1}}}}),
	NewPattern(Object{"a": Array{Object{"a": 1}, Object{"b": 1}, Object{"c": 1}}}),
	NewPattern(Object{"a": 1, "b": 1, "c": 1, "d": 1}),
	NewPattern(Object{"a": Array{Object{"b": 1}}, "c": Array{Object{"d": 1}}}),
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
	match := NewPattern(Object{"a": Array{Object{"a": 1}, Object{"b": 1}, Object{"c": 1}}})
	size := len(patterns)
	for i := 0; i < b.N; i += 1 {
		for j := 0; j < size; j += 1 {
			if patterns[j].Equals(match) {
			}
		}
	}
}
func BenchmarkPattern_MatchString(b *testing.B) {
	match := NewPattern(Object{"a": Array{Object{"a": 1}, Object{"b": 1}, Object{"c": 1}}})
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
