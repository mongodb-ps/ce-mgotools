// MongoDB logging has its own special blend of JSON output, i.e. not at all
// standard compliant. Any standard JSON parsers will surely fail. This is
// especially true with things like non-JSON data types (there are only six,
// after all). That means a custom JSON parser is necessary to accurately grab
// all the necessary information.

package mongo

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"

	"mgotools/internal"
	"mgotools/util"
)

// https://docs.mongodb.com/manual/reference/mongodb-extended-json/

func ParseJson(json string, strict bool) (map[string]interface{}, error) {
	return ParseJsonRunes(util.NewRuneReader(json), strict)
}

func ParseJsonRunes(r *util.RuneReader, strict bool) (map[string]interface{}, error) {
	if r.Length() < 2 {
		return nil, fmt.Errorf("json must contain at least two characters")
	}
	v, e := parseJson(r.ChompWS(), strict)
	if strict && !r.EOL() {
		return nil, fmt.Errorf("unexpected character '%c' at %d", r.NextRune(), r.Pos())
	}
	//util.Debug("\nJSON: %+v\nJSON error: %+v\n[%s]\n", v, e, r.String())
	return v, e
}

func parseJson(r *util.RuneReader, strict bool) (map[string]interface{}, error) {
	var data = make(map[string]interface{})
	if current := r.NextRune(); current != '{' {
		return nil, fmt.Errorf("expected '{' but found '%c'", current)
	} else {
		r.Skip(1)
	}
	for {
		// Skip empty whitespaces.
		if r.ChompWS().EOL() {
			// End parsing when end of string reached.
			return nil, fmt.Errorf("unexpected end of string")
		}
		keyOffset := r.Pos()
		current := r.NextRune()
		if current == '}' {
			// End parsing and return data when closing character found.
			r.Skip(1)
			r.ChompWS()
			return data, nil
		} else if key, err := parseKey(r, strict); err != nil {
			return nil, err
		} else if size := len(key); unicode.IsPunct(rune(key[size-1])) {
			return nil, fmt.Errorf("unexpected character '%c' at %d", key[size-1], size)
		} else {
			// Skip empty white spaces before the colon.
			if r.ChompWS().NextRune() != ':' {
				return nil, fmt.Errorf("unexpected character '%c' at %d", r.NextRune(), r.Pos())
			} else {
				r.Next()
				r.ChompWS()
			}

			// Keep the value offset in case changes must be made to the value
			// (like in cases where there's an unescaped string).
			valueOffset := r.Pos()
			if data[key], err = parseValue(r, strict); err != nil {
				return nil, err
			}
			if r.ChompWS().NextRune() == ',' {
				r.Skip(1)
				continue
			} else if r.NextRune() == '}' {
				r.Skip(1)
				r.ChompWS()
				return data, nil
			} else if !strict && !r.EOL() {
				// This section exists to handle unquoted string characters.
				if _, ok := data[key].(string); ok {
					r.Seek(valueOffset, 0)
					if s, err := r.QuotedString(); err == nil && s == data[key] {
						r.Insert('\\', valueOffset+len(s)+1)
						r.Seek(keyOffset, 0)
						continue
					}
				}
			}
			return nil, fmt.Errorf("unexpected character '%c' after value at %d", r.NextRune(), r.Pos())
		}
	}
}

func checkRune(r rune, a ...interface{}) bool {
	for _, b := range a {
		switch v := b.(type) {
		case rune:
			return r == v
		case []rune:
			for _, i := range v {
				if r == i {
					return true
				}
			}
		case byte:
		case int:
			if r == rune(v) {
				return true
			}
		case *unicode.RangeTable:
			if unicode.Is(v, r) {
				return true
			}
		default:
			panic(fmt.Sprintf("unexpected match type: %T", v))
		}
	}
	return false
}

func parseArray(r *util.RuneReader, strict bool) ([]interface{}, error) {
	var (
		c      rune
		ok     bool = true
		values      = make([]interface{}, 0, 16)
	)
	if c, _ := r.Next(); c != '[' {
		panic("unexpected character [")
	}
	if r.ChompWS().NextRune() == ']' {
		// The array is empty so skip the closing bracket (]) and return an empty interface.
		r.Next()
		return values, nil
	}
	for c = ','; ok && c == ','; c, ok = r.Next() {
		if next, err := parseValue(r.ChompWS(), strict); err != nil {
			return nil, err
		} else {
			values = append(values, next)
			if r.ChompWS().NextRune() == ']' {
				r.Next()
				return values, nil
			}
		}
	}
	return nil, fmt.Errorf("unexpected character '%c' at %d in array", r.NextRune(), r.Pos())
}

func parseKey(r *util.RuneReader, strict bool) (string, error) {
	var (
		err error
		key string
	)
	switch c := r.NextRune(); c {
	case ',':
		return "", fmt.Errorf("unexpected character ',' at %d", r.Pos())
	case '"':
		if key, err = r.QuotedString(); err != nil {
			return "", err
		}
		if strict {
			for index, letter := range []rune(key) {
				// Check that a dollar sign ($) does not exist after the first character.
				if letter == '.' || (index > 0 && letter == '$') {
					return "", fmt.Errorf("restricted character '%c' cannot exist in key", letter)
				}
			}
		}
		return string(key), nil
	case '$':
		if strict {
			// A dollar sign cannot start a key name unless it is quoted.
			return "", fmt.Errorf("unquoted field names cannot begin with a $ character")
		}
		fallthrough
	default:
		if strict {
			return "", fmt.Errorf("unquoted keys are not allowed in strict mode")
		}
		if !checkRune(c, unicode.Letter, []rune{'$', '_'}) {
			return "", fmt.Errorf("first character in key must be a letter, dollar sign ($), or underscore (_)")
		}
		if strict {
			for r.Expect(unicode.Letter, unicode.Number, []rune{'$', '_', '.'}) {
				r.Next()
			}
		} else {
			if _, ok := r.ScanFor([]rune{':', 0}, unicode.White_Space); !ok {
				return "", fmt.Errorf("reached end of string while parsing key")
			}
			if r, _ := r.Prev(); r == 0 {
				return "", fmt.Errorf("null character found in key")
			}
		}
	}
	return r.CurrentWord(), nil
}

// https://docs.mongodb.com/manual/reference/limits/
// https://github.com/mongodb/mongo/blob/master/src/mongo/bson/json.cpp
func parseValue(r *util.RuneReader, strict bool) (interface{}, error) {
	var (
		err   error
		value interface{}
	)
	switch c := r.NextRune(); {
	case c == '{': // Object
		if value, err = parseJson(r, strict); err != nil {
			value = parseDataType(value.(map[string]interface{}))
		}
	case c == '[': // Array
		value, err = parseArray(r, strict)
	case c == '\'': // Single quoted string
		if strict {
			return nil, fmt.Errorf("unexpected character '%c' not allowed in strict mode at %d", c, r.Pos())
		}
		fallthrough
	case c == '"': // Double quoted string
		value, err = r.QuotedString()
	case c == '/': // Regular expression
		if value, err = r.EnclosedString('/', true); err != nil {
			return "", err
		}
		if options, ok := r.ScanWhile([]rune{'i', 'g', 'x'}); ok { // TODO: POPULATE OTHER OPTIONS
			value = Regex{value.(string), options}
		} else {
			value = Regex{value.(string), ""}
		}
	case unicode.IsLetter(c):
		for ok := true; ok && !checkRune(c, unicode.Space, []rune{',', '}'}); c, ok = r.Next() {
		}
		r.Prev()
		switch word := strings.ToLower(r.CurrentWord()); word {
		case "true":
			value = true
		case "false":
			value = false
		case "null":
			value = nil
		case "MaxKey":
			value = MaxKey{}
		case "MinKey":
			value = MinKey{}
		case "timestamp":
			value, err = parseTimestamp(r)
		case "undefined":
			value = Undefined{}
		default:
			length := util.StringLength(word)
			if length == 36 && word[:8] == "objectid" {
				value, err = parseObjectId(word, strict)
			} else if length == 5 && word[:5] == "dbref" {
				r.Prev()
				value, err = parseDbRef(r)
			} else if length == 3 && word == "new" {
				value, err = parseDate(r)
			} else if length > 7 && word[:7] == "bindata" {
				r.RewindSlurpWord()
				value, err = parseBinData(r)
			} else {
				return nil, fmt.Errorf("unrecognized type beginning with '%c' at %d", r.NextRune(), r.Pos())
			}
		}
	case r.Expect(unicode.Digit, '-', '+', '.'):
		if value, err = parseNumber(r); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected value at %d, got '%c'", r.Pos(), r.NextRune())
	}
	return value, err
}

func parseBinData(r *util.RuneReader) (BinData, error) {
	// BinData(3, 7B41B8989CD93C2E80E3AA19F4732581)
	// The RuneReader should be pointing to the beginning of the word.
	var b BinData

	// Skip "BinData("
	r.Skip(8)

	// Get the type (should be an integer).
	t, ok := r.SlurpWord()
	if !ok || len(t) < 2 {
		return b, internal.UnexpectedEOL
	} else if num, err := strconv.ParseInt(t[:len(t)-1], 10, 8); err != nil {
		return BinData{}, err
	} else {
		b.Type = byte(num)
	}

	// Get the data itself.
	c, ok := r.SlurpWord()
	if !ok || len(c) < 2 {
		return BinData{}, internal.UnexpectedEOL
	} else if hex, err := hex.DecodeString(c[:len(c)-1]); err != nil {
		// Decode the string into hex and set the output.
		return BinData{}, err
	} else {
		b.BinData = hex
	}

	return b, nil
}

func parseDataType(m map[string]interface{}) interface{} {
	switch len(m) {
	case 1:
		if _, ok := m["$date"].(time.Time); ok {
			return m["$date"]
		} else if _, ok := m["$timestamp"].(time.Time); ok {
			return Timestamp(m["$timestamp"].(time.Time))
		} else if _, ok := m["$oid"].(string); ok {
			oid, _ := NewObjectId(m["$oid"].(string))
			return oid
		} else if _, ok := m["$undefined"].(bool); ok && m["$undefined"].(bool) {
			return Undefined{}
		} else if _, ok := m["$minKey"].(int); ok && m["$minKey"] == 1 {
			return MinKey{}
		} else if _, ok := m["$maxKey"]; ok && m["$maxKey"] == 1 {
			return MaxKey{}
		} else if _, ok := m["$numberLong"].(int64); ok {
			return m["$numberLong"]
		} else if _, ok := m["$numberDecimal"].(float64); ok {
			return m["$numberDecimal"]
		} else if _, ok := m["$regex"].(string); ok {
			return Regex{m["$regex"].(string), ""}
		}
	case 2:
		if _, ok := m["$binary"].([]byte); ok {
			if t, ok := m["$type"].(string); ok {
				if h, err := hex.DecodeString(t); err == nil && len(h) == 1 {
					return BinData{m["$binary"].([]byte), h[0]}
				}
			}
		} else if _, ok := m["$regex"]; ok {
			if _, ok := m["$options"].(string); ok {
				return Regex{m["$regex"].(string), m["$options"].(string)}
			}
		} else if _, ok := m["$ref"].(string); ok {
			if _, ok := m["$id"].(string); ok {
				oid, _ := NewObjectId(m["$id"].(string))
				return Ref{m["$ref"].(string), oid}
			}
		}
	}
	return m
}

func parseDate(r *util.RuneReader) (time.Time, error) {
	// new Date(1490821611611)
	if r.CurrentWord() == "new" {
		r.SkipWords(1)
	}
	// Date(1524927048785)
	offset := 0
	if util.StringInsensitiveMatch(r.Peek(5), "date(") {
		offset = 5
	} else if util.StringInsensitiveMatch(r.Peek(8), "isodate(") {
		offset = 8
	} else {
		return time.Time{}, internal.UnexpectedEOL
	}
	r.Skip(offset)
	if date := r.Peek(13); len(date) != 13 {
		return time.Time{}, fmt.Errorf("unrecognized date string (%s)", date)
	} else if t, err := strconv.ParseInt(date, 10, 64); err != nil {
		return time.Time{}, err
	} else {
		r.Skip(13)
		if end, _ := r.Next(); end != ')' {
			return time.Time{}, fmt.Errorf("unexpected character '%c' in date string at %d", end, r.Pos())
		}
		return time.Unix(t/1000, (t%1000)*1000000), nil
	}
}

func parseDbRef(r *util.RuneReader) (Ref, error) {
	if !r.ExpectString("DBRef(") {
		return Ref{}, fmt.Errorf("unexpected word at %d", r.Pos())
	}

	ref := Ref{}
	r.Skip(6)
	if id, err := r.QuotedString(); err != nil {
		return Ref{}, err
	} else {
		ref.Name = id
	}

	// Skip ", "
	r.Skip(2)
	if oid, ok := r.ScanFor(')'); !ok {
		return Ref{}, fmt.Errorf("unexpected end of string")
	} else if l := len(oid); l != 25 {
		return Ref{}, fmt.Errorf("unexpected OID format")
	} else {
		ref.Id, ok = NewObjectId(oid[:24])
		if !ok {
			return Ref{}, fmt.Errorf("cannot translate from hex to objectid")
		}
		return ref, nil
	}
}

func parseNumber(r *util.RuneReader) (interface{}, error) {
	char, ok := r.Next()
	if !ok {
		return nil, fmt.Errorf("unexpected end of string")
	}
	f64 := false
	for ; ok && checkRune(char, unicode.Digit, []rune{'-', '+', '.', 'e', 'E'}); char, ok = r.Next() {
		switch char {
		case 'e', 'E', '.':
			f64 = true
		}
	}
	if ok {
		r.Prev()
	}
	// TODO: decimal128 support needed
	if f64 {
		return strconv.ParseFloat(r.CurrentWord(), 64)
	} else {
		if v, err := strconv.ParseInt(r.CurrentWord(), 10, 64); err != nil {
			return nil, err
		} else {
			if v < math.MinInt32 || v > math.MaxInt32 {
				return int64(v), nil
			} else {
				return int(v), nil
			}
		}
	}
}

func parseObjectId(oid string, strict bool) (value ObjectId, err error) {
	// ObjectId('59e3fdf682f5ead28303a9cb')
	if util.StringLength(oid) != 36 {
		return ObjectId{}, internal.UnexpectedLength
	}
	if (strict && !strings.HasPrefix(oid, "ObjectId(")) || (!strict && !util.StringInsensitiveMatch(oid[:9], "objectid(")) {
		return ObjectId{}, internal.MisplacedWordException
	}
	encoded := oid[10:34]
	if decoded, err := hex.DecodeString(encoded); err != nil {
		return ObjectId{}, err
	} else {
		copy(value[:], decoded[:12])
	}
	return
}

func parseTimestamp(r *util.RuneReader) (time.Time, error) {
	// The log format is "Timestamp 0|0". The "Timestamp" portion should already
	// be removed from the reader so continue parsing forward.

	// Start by removing any extra spaces from before the value.
	r.ChompWS()

	// Next scan until a space, comma, curly bracket indicating end of value.
	if term, ok := r.ScanFor(unicode.White_Space, ',', '}'); term == "" && !ok {
		return time.Time{}, internal.UnexpectedEOL
	}

	// Rewind one character (a space, comma, or curly bracket).
	r.Prev()

	// Split into two parts and parse the values into integers.
	if swall, sns, ok := util.StringDoubleSplit(r.CurrentWord(), '|'); !ok {
		return time.Time{}, internal.MisplacedWordException
	} else if wall, err := strconv.ParseUint(swall, 10, 32); err != nil {
		return time.Time{}, internal.UnexpectedValue
	} else if ns, err := strconv.ParseUint(sns, 10, 32); err != nil {
		return time.Time{}, internal.UnexpectedValue
	} else {
		// The wall clock portion gets converted and eventually stored as a uint.
		return time.Unix(int64(wall), int64(ns)), nil
	}
}
