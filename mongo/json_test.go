package mongo

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"mgotools/internal"
)

func TestParseJson(t *testing.T) {
	// These tests should pass both strict and weak JSON tests.
	s1 := map[string]map[string]interface{}{
		"{}":                                     {},
		`{"key":"value"}`:                        {"key": "value"},
		`{"$key":"value"}`:                       {"$key": "value"},
		`{    "key"   :    "value"    }`:         {"key": "value"},
		`{    "key"   :    "value"    }        `: {"key": "value"},
		`{"key1":"value","key2":"value"}`:        {"key1": "value", "key2": "value"},
		`{"key1" : "value" , "key2" : "value" }`: {"key1": "value", "key2": "value"},
		`{"key":true}`:                           {"key": true},
		`{"key":false}`:                          {"key": false},
		`{"key":"true"}`:                         {"key": "true"},
		`{"number" : 1}`:                         {"number": 1},
		`{"float" : 1.5}`:                        {"float": 1.5},
		`{"object":{"key":"value"}}`:             {"object": map[string]interface{}{"key": "value"}},
		`{"key":[]}`:                             {"key": []interface{}{}},
		`{"key": ["value"]}`:                     {"key": []interface{}{"value"}},
		`{"key":[ "value1" , "value2" ]}`:        {"key": []interface{}{"value1", "value2"}},
		`{"key": /regex/ }`:                      {"key": Regex{"regex", ""}},
		`{"key": /regex/i }`:                     {"key": Regex{"regex", "i"}},
		`{"key": /(?:)/i }`:                      {"key": Regex{"(?:)", "i"}},
		`{"key": Timestamp 492000|16}`:           {"key": time.Unix(492000, 16)},
		`{"key":Timestamp 0|0}`:                  {"key": time.Unix(0, 0)},
		`{"object":{"key1":"value1" , "key2" : "value2" } }`: {"object": map[string]interface{}{"key1": "value1", "key2": "value2"}},
	}

	for source, target := range s1 {
		if value, err := ParseJson(source, false); err != nil {
			t.Errorf("Json failed (%s): %s", source, err)
		} else if !reflect.DeepEqual(value, target) {
			t.Errorf("Json mismatch at ([%T] %+v, expected [%T] %+v)", value, value, target, target)
		}
		if value, err := ParseJson(source, true); err != nil {
			t.Errorf("Json failed with strict mode (%s): %s", source, err)
		} else if !reflect.DeepEqual(value, target) {
			t.Errorf("Json mismatch at ([%T] %+v, expected [%T] %+v)", value, value, target, target)
		}
	}

	// These should only pass weak tests.
	s2 := map[string]map[string]interface{}{
		`{    key:"value"}`:      {"key": "value"},
		`{"key.key":"value"}`:    {"key.key": "value"},
		`{ $key: "value" }`:      {"$key": "value"},
		`{ key.1: "value" } `:    {"key.1": "value"},
		`{ key.name : "value" }`: {"key.name": "value"},
		`{key:{$op:"value"}}`:    {"key": map[string]interface{}{"$op": "value"}},
		`{key:"value"}`:          {"key": "value"},
		`{"key":''}`:             {"key": ""},
		`{"key": objectid(00000000000000000000000000)}`: {"key": ObjectId{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		`{"key": "this " is " incorrectly " quoted"}`:   {"key": `this \" is \" incorrectly \" quoted`},
	}

	for source, target := range s2 {
		if value, err := ParseJson(source, false); err != nil {
			t.Errorf("Json failed (%s): %s", source, err)
		} else if !reflect.DeepEqual(value, target) {
			t.Errorf("Json mismatch at ([%T] %+v, expected [%T] %+v)", value, value, target, target)
		}
		if _, err := ParseJson(source, true); err == nil {
			t.Errorf("Strict test passed incorrectly (%s): %s", source, err)
		}
	}

	// All of these tests should fail both strict and weak mode.
	n1 := []string{
		`[`,
		`[]`,
		`{:true}`,
		`{"a":}'`,
		`{"a":1,`,
		`{"a":1 b:1}`,
		`{"a":1, "b:1}`,
		`{"a" 1}`,
		`{$`,
		`{"a":1,,}`,
		`{"a":{`,
		`{"a":/a`,
		`{"key": unknown}`,
		`{"key": -1-}`,
		`{"key": objectid(00)}`,
		`{key.1.:"value"}`,
		`{key: Timestamp -1|0}`,
		`{key: Timestamp 0|-1}`,
		`{key: Timestamp 0}`,
		`{key: 4294967296|4294967296}`,
	}

	for _, str := range n1 {
		if _, err := ParseJson(str, false); err == nil {
			t.Errorf("expected error, got none: %s", str)
		}
		if _, err := ParseJson(str, true); err == nil {
			t.Errorf("expected error in strict mode, got none: %s", str)
		}
	}
}

func TestParseDataType(t *testing.T) {
	m := []map[string]interface{}{
		{"$date": time.Now()},
		{"$timestamp": time.Now()},
		{"$oid": "1234567890abcdef"},
		{"$undefined": true},
		{"$minKey": 1},
		{"$maxKey": 1},
		{"$numberLong": int64(1)},
		{"$numberDecimal": float64(1.0)},
		{"$regex": "/abc/"},
		{"$binary": []byte{0xde, 0xad, 0xbe, 0xef}, "$type": "00"},
		{"$regex": "/abc/", "$options": "i"},
		{"$ref": "abc", "$id": "_id"},
	}
	for index, v := range m {
		c := parseDataType(v)
		if reflect.DeepEqual(c, v) {
			t.Errorf("Extended type conversion at %d failed (%T %v, %T %v)", index, v, v, c, c)
		}
	}
}

func TestBinData(t *testing.T) {
	bin, err := parseBinData(internal.NewRuneReader("BinData(0, 0123456789ABCDEF)"))
	check := BinData{[]byte{0x1, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF}, 0x0}
	if err != nil || !bytes.Equal(bin.BinData, check.BinData) || bin.Type != check.Type {
		t.Errorf("Expected (%x, %x), got (%x, %x)", check.BinData, check.Type, bin.BinData, bin.Type)
	}
	bin, err = parseBinData(internal.NewRuneReader("BinData(, 0123456789ABCDEF)"))
	if err == nil {
		t.Error("Expected EOF error, got none")
	}

	bin, err = parseBinData(internal.NewRuneReader("BinData(256, 0123456789ABCDEF)"))
	if err == nil {
		t.Error("Expected overflow error, got none")
	}

	bin, err = parseBinData(internal.NewRuneReader("BinData(0, )"))
	if err == nil {
		t.Error("Expected EOF error, got none")
	}

	bin, err = parseBinData(internal.NewRuneReader("BinData(0, x)"))
	if err == nil {
		t.Error("Expected EOF error, got none")
	}
}

func TestParseDbRef(t *testing.T) {
	ref, err := parseDbRef(internal.NewRuneReader("DBRef('test', 0123456789abcdef01234567)"))
	tid, _ := NewObjectId("0123456789abcdef01234567")

	if err != nil || ref.Name != "test" || !ref.Id.Equals(tid) {
		t.Errorf("Expected (test, 012345678901234567890123), got (%s, %s) %s", ref.Name, ref.Id, err)
	}

	_, err = parseDbRef(internal.NewRuneReader("ObjectId()"))
	if err == nil {
		t.Errorf("Expected DBRef error, got none")
	}

	ref, err = parseDbRef(internal.NewRuneReader("DBRef(xyz, 012345678901234567890123)"))
	if err == nil {
		t.Errorf("Expected DBRef error, got none")
	}

	ref, err = parseDbRef(internal.NewRuneReader("DBRef('test', 01234567890123456789012"))
	if err == nil {
		t.Errorf("Expected DBRef error, got none")
	}

	ref, err = parseDbRef(internal.NewRuneReader("DBRef('test', 0123456789012345678901234)"))
	if err == nil {
		t.Errorf("Expected DBRef error, got none")
	}

	ref, err = parseDbRef(internal.NewRuneReader("DBRef('test', abcdefg/:`0123456789012345)"))
	if err == nil {
		t.Errorf("Expected DBRef error, got none")
	}
}

func TestParseNumber(t *testing.T) {
	m := map[string]interface{}{
		"1":           1,
		"-1":          -1,
		"0.1":         0.1,
		"-0.1":        -0.1,
		"10e2":        float64(1000),
		"-10e2":       float64(-1000),
		"1.5e2":       float64(150),
		"-1.5e2":      float64(-150),
		"-2147483648": int(-2147483648),
		"2147483647":  int(2147483647),
		"2147483648":  int64(2147483648),
		"-2147483649": int64(-2147483649),
		"1 ":          1,
	}
	for s, v := range m {
		if c, err := parseNumber(internal.NewRuneReader(s)); c != v || err != nil {
			t.Errorf("Parsing number '%s' (%T %v) returned %T %v: %s", s, v, v, c, c, err)
		}
	}
}

func TestParseJsonRunes(t *testing.T) {
	r := internal.NewRuneReader("{a:1}")
	if s, err := ParseJsonRunes(r, false); err != nil || !reflect.DeepEqual(s, map[string]interface{}{"a": 1}) {
		t.Errorf("Rune parsing failed, returned: %#v", s)
	}

	r = internal.NewRuneReader("{a:1}x")
	if s, err := ParseJsonRunes(r, false); err != nil || !reflect.DeepEqual(s, map[string]interface{}{"a": 1}) {
		t.Errorf("Rune parsing failed, returned: %#v", s)
	} else if !r.ExpectRune('x') {
		t.Errorf("Expected 'x', got '%s'", r.Peek(1))
	}

	r = internal.NewRuneReader("{a:1} x")
	if s, err := ParseJsonRunes(r, false); err != nil || !reflect.DeepEqual(s, map[string]interface{}{"a": 1}) {
		t.Errorf("Rune parsing failed, returned: %#v", s)
	} else if !r.ExpectRune('x') {
		t.Errorf("Expected 'x', got '%s'", r.Peek(1))
	}
}

func TestCheckRune(t *testing.T) {
	if !checkRune('a', 'a') ||
		!checkRune('a', []rune{'a'}) ||
		!checkRune('a', int('a')) {
		t.Errorf("unexpected character mismatch")
	}
}
