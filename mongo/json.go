package mongo

import (
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"math"
	"mgotools/util"
	"strconv"
	"strings"
	"time"
	"unicode"
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
	//Debug("\nJSON: %+v\nJSON error: %+v [next '%s']\n", v, e, r.PreviewWord(1))
	return v, e
}

func parseJson(r *util.RuneReader, strict bool) (map[string]interface{}, error) {
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
		}
		current := r.NextRune()
		if current == '}' {
			// End parsing and return data when closing character found.
			r.Skip(1)
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
				r.Skip(1)
			} else if r.NextRune() == '}' {
				r.Skip(1)
				return data, nil
			} else {
				return nil, fmt.Errorf("unexpected character '%c' after value at %d", r.NextRune(), r.Pos())
			}
		}
	}
}

func parseArray(r *util.RuneReader, strict bool) ([]interface{}, error) {
	var (
		c      rune
		ok     bool          = true
		values []interface{} = make([]interface{}, 0, 16)
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
		for r.Expect(unicode.Letter, unicode.Number, []rune{'$', '_'}) {
			r.Next()
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
		if value, err = r.EnclosedString('/'); err != nil {
			return "", err
		}
		if options, ok := r.ScanForRuneWhile([]rune{'i', 'g', 'x'}); ok { // TODO: POPULATE OTHER OPTIONS
			value = Regex{value.(string), options}
		} else {
			value = Regex{value.(string), ""}
		}
	case unicode.IsLetter(c):
		for ok := true; ok && !checkRune(c, unicode.Space, []rune{',', '}'}); c, ok = r.Next() {
		}
		r.Prev()
		switch word := r.CurrentWord(); word {
		case "true":
			value = true
		case "false":
			value = false
		default:
			if util.StringLength(word) == 36 && strings.ToLower(word[:8]) == "objectid" {
				value, err = parseObjectId(word, strict)
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

func parseObjectId(oid string, strict bool) (ObjectId, error) {
	// ObjectId('59e3fdf682f5ead28303a9cb')
	if util.StringLength(oid) != 36 {
		return nil, errors.New("unexpected length")
	}
	if (strict && !strings.HasPrefix(oid, "ObjectId(")) || (!strict && !util.StringInsensitiveMatch(oid[:9], "objectid(")) {
		return nil, errors.New("unexpected string")
	}
	encoded := oid[10:34]
	if decoded, err := hex.DecodeString(encoded); err != nil {
		return nil, err
	} else {
		return decoded, nil
	}
}

func parseDataType(m map[string]interface{}) interface{} {
	switch len(m) {
	case 1:
		if _, ok := m["$date"].(time.Time); ok {
			return m["$date"]
		} else if _, ok := m["$timestamp"].(time.Time); ok {
			return Timestamp(m["$timestamp"].(time.Time))
		} else if _, ok := m["$oid"].(string); ok {
			return ObjectId(m["$oid"].(string))
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
			if _, ok := m["$type"].(string); ok {
				return BinData{m["$binary"].([]byte), m["$type"].(string)}
			}
		} else if _, ok := m["$regex"]; ok {
			if _, ok := m["$options"].(string); ok {
				return Regex{m["$regex"].(string), m["$options"].(string)}
			}
		} else if _, ok := m["$ref"].(string); ok {
			if _, ok := m["$id"]; ok {
				return Ref{m["$ref"].(string), m["$id"]}
			}
		}
	}
	return m
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
