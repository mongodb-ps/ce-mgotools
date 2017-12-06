package util

import (
	"fmt"
	"math"
	"mgotools/mongo"
	"strconv"
	"time"
	"unicode"
)

// https://docs.mongodb.com/manual/reference/mongodb-extended-json/

func ParseJson(json string, strict bool) (map[string]interface{}, error) {
	return ParseJsonRunes(NewRuneReader(json), strict)
}

func ParseJsonRunes(r *RuneReader, strict bool) (map[string]interface{}, error) {
	if r.Length() < 2 {
		return nil, fmt.Errorf("json must contain at least two characters")
	}
	v, e := parseJson(r.ChompWS(), strict)
	r.Skip(1).ChompWS()
	if strict && !r.EOL() {
		return nil, fmt.Errorf("unexpected character '%c' at %d", r.NextRune(), r.Pos())
	}
	Debug("Original value: %s\nFinal value: %+v\nFinal error: %+v", string(r.runes), v, e)
	return v, e
}

func parseJson(r *RuneReader, strict bool) (map[string]interface{}, error) {
	var data map[string]interface{} = make(map[string]interface{})
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
		} else {
			current := r.NextRune()
			if current == '}' {
				// End parsing and return data when closing character found.
				return data, nil
			} else if key, err := parseKey(r, strict); err != nil {
				return nil, err
			} else {
				// Skip empty white spaces before the colon.
				if r.ChompWS().NextRune() != ':' {
					return nil, fmt.Errorf("unexpected character '%c' at %d", r.NextRune(), r.Pos())
				} else {
					r.Next()
					r.ChompWS()
				}
				if data[key], err = parseValue(r, strict); err != nil {
					return nil, err
				}
				if r.ChompWS().NextRune() == ',' {
					r.Next()
				} else if r.NextRune() == '}' {
					r.Next()
					return data, nil
				} else {
					return nil, fmt.Errorf("unexpected character '%c' after value at %d", r.NextRune(), r.Pos())
				}
			}
		}
	}
}

func parseArray(r *RuneReader, strict bool) ([]interface{}, error) {
	var (
		c      rune
		ok     bool          = true
		values []interface{} = make([]interface{}, 0, 16)
	)
	if c, _ := r.Next(); c != '[' {
		panic("unexpected character [")
	}
	for c = ','; ok && c == ','; c, ok = r.Next() {
		next, err := parseValue(r.ChompWS(), strict)
		values = append(values, next)
		if r.ChompWS().NextRune() == ']' {
			r.Next()
			return values, err
		}
	}
	return nil, fmt.Errorf("unexpected character '%c' at %d in array", r.NextRune(), r.Pos())
}

func parseKey(r *RuneReader, strict bool) (string, error) {
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
		if !checkRune(c, unicode.Letter, []rune{'$', '_'}) {
			return "", fmt.Errorf("first character in key must be a letter, dollar sign ($), or underscore (_)")
		}
		for checkReader(r, unicode.Letter, unicode.Number, []rune{'$', '_'}) {
			if _, ok := r.Next(); !ok {
				return "", fmt.Errorf("unexpected end of string")
			}
		}
	}
	return string(r.runes[r.start:r.next]), nil
}

// https://docs.mongodb.com/manual/reference/limits/
// https://github.com/mongodb/mongo/blob/master/src/mongo/bson/json.cpp
func parseValue(r *RuneReader, strict bool) (interface{}, error) {
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
		if value, err = r.EnclosedString('/'); err != nil {
			return "", err
		}
		if options, ok := r.ScanForRuneWhile([]rune{'i', 'g', 'x'}); ok { // TODO: POPULATE OTHER OPTIONS
			value = mongo.Regex{value.(string), options}
		} else {
			value = mongo.Regex{value.(string), ""}
		}
	case unicode.IsLetter(c):
		for ok := true; ok && !checkRune(c, unicode.Space, ',', '}'); c, ok = r.Next() {
		}
		r.Prev()
		switch r.CurrentWord() {
		case "true":
			value = true
		case "false":
			value = false
		default:
			return nil, fmt.Errorf("unrecognized type beginning with '%c' at %d", r.NextRune(), r.Pos())
		}
	case checkReader(r, unicode.Digit, '-', '+', '.'):
		if value, err = parseNumber(r); err != nil {
			return value, nil
		}
	default:
		return nil, fmt.Errorf("unexpected value at %d, got '%c'", r.Pos(), r.NextRune())
	}
	return value, err
}

func parseNumber(r *RuneReader) (interface{}, error) {
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

func parseDataType(m map[string]interface{}) interface{} {
	switch len(m) {
	case 1:
		if _, ok := m["$date"].(time.Time); ok {
			return m["$date"]
		} else if _, ok := m["$timestamp"].(time.Time); ok {
			return mongo.Timestamp(m["$timestamp"].(time.Time))
		} else if _, ok := m["$oid"].(string); ok {
			return mongo.ObjectId(m["$oid"].(string))
		} else if _, ok := m["$undefined"].(bool); ok && m["$undefined"].(bool) {
			return mongo.Undefined{}
		} else if _, ok := m["$minKey"].(int); ok && m["$minKey"] == 1 {
			return mongo.MinKey{}
		} else if _, ok := m["$maxKey"]; ok && m["$maxKey"] == 1 {
			return mongo.MaxKey{}
		} else if _, ok := m["$numberLong"].(int64); ok {
			return m["$numberLong"]
		} else if _, ok := m["$numberDecimal"].(float64); ok {
			return m["$numberDecimal"]
		} else if _, ok := m["$regex"].(string); ok {
			return mongo.Regex{m["$regex"].(string), ""}
		}
	case 2:
		if _, ok := m["$binary"].([]byte); ok {
			if _, ok := m["$type"].(string); ok {
				return mongo.BinData{m["$binary"].([]byte), m["$type"].(string)}
			}
		} else if _, ok := m["$regex"]; ok {
			if _, ok := m["$options"].(string); ok {
				return mongo.Regex{m["$regex"].(string), m["$options"].(string)}
			}
		} else if _, ok := m["$ref"].(string); ok {
			if _, ok := m["$id"]; ok {
				return mongo.Ref{m["$ref"].(string), m["$id"]}
			}
		}
	}
	return m
}
