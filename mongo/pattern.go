package mongo

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"mgotools/util"
)

type Pattern struct {
	pattern     Object
	initialized bool
}
type V struct {
}

func NewPattern(s map[string]interface{}) Pattern {
	return Pattern{createPattern(s, false), true}
}
func (p *Pattern) IsEmpty() bool {
	return !p.initialized
}
func (p *Pattern) Equals(object Pattern) bool {
	return deepEqual(p.pattern, object.pattern)
}
func (p *Pattern) Pattern() map[string]interface{} {
	return p.pattern
}
func (p *Pattern) String() string {
	return createString(p, false)
}
func (p *Pattern) StringCompact() string {
	return createString(p, true)
}
func createPattern(s map[string]interface{}, expr bool) map[string]interface{} {
	compress := func(t map[string]interface{}) interface{} {
		for key := range t {
			if !util.ArrayInsensitiveMatchString(OPERATORS_COMPARISON, key) {
				return t
			}
		}
		return V{}
	}

	for key := range s {
		switch t := s[key].(type) {
		case map[string]interface{}:
			if !expr || util.ArrayInsensitiveMatchString(OPERATORS_COMPARISON, key) {
				s[key] = compress(createPattern(t, true))
			} else if util.ArrayInsensitiveMatchString(OPERATORS_LOGICAL, key) || util.ArrayInsensitiveMatchString(OPERATORS_EXPRESSION, key) {
				s[key] = createPattern(t, false)
			} else {
				s[key] = V{}
			}

		case []interface{}:
			if util.ArrayInsensitiveMatchString(OPERATORS_LOGICAL, key) {
				s[key] = createArray(t, false)
			} else if util.ArrayInsensitiveMatchString(OPERATORS_EXPRESSION, key) {
				s[key] = createArray(t, true)
			} else {
				s[key] = V{}
			}

		default:
			s[key] = V{}
		}
	}
	return s
}

func createString(p *Pattern, compact bool) string {
	if !p.initialized {
		return ""
	}
	var arr func([]interface{}) string
	var obj func(map[string]interface{}) string
	arr = func(array []interface{}) string {
		var buffer = bytes.NewBufferString("[")
		total := len(array)
		v := 0

		if !compact && total > 0 {
			buffer.WriteRune(' ')
		}

		for index := 0; index < total; index += 1 {
			r := array[index]
			switch t := r.(type) {
			case []interface{}:
				r := arr(t)
				if r == "1" {
					v += 1
				}

				buffer.WriteString(r)

			case map[string]interface{}:
				buffer.WriteString(obj(t))

			case V:
				buffer.WriteRune('1')
				v += 1

			default:
				panic(fmt.Sprintf("unexpected type %T in pattern", r))
			}
			if index < total-1 {
				buffer.WriteString(", ")
			} else if !compact {
				buffer.WriteRune(' ')
			}
		}
		if v == total {
			return "1"
		}
		buffer.WriteRune(']')
		return buffer.String()
	}
	obj = func(object map[string]interface{}) string {
		var buffer = bytes.NewBuffer([]byte{'{'})
		total := len(object)
		count := 0
		if !compact && total > 0 {
			buffer.WriteRune(' ')
		}
		keys := make(keySorter, 0)
		for key := range object {
			keys = append(keys, key)
		}
		sort.Sort(keys)
		for _, key := range keys {
			count += 1
			buffer.WriteRune('"')
			buffer.WriteString(key)
			buffer.WriteRune('"')
			buffer.WriteString(": ")

			switch t := object[key].(type) {
			case []interface{}:
				buffer.WriteString(arr(t))

			case map[string]interface{}:
				buffer.WriteString(obj(t))

			case V:
				buffer.WriteRune('1')
			}

			if count < total {
				buffer.WriteString(", ")
			} else if !compact {
				buffer.WriteRune(' ')
			}
		}

		buffer.WriteRune('}')
		return buffer.String()
	}
	return obj(p.pattern)
}

func createArray(t []interface{}, expr bool) interface{} {
	size := len(t)
	v := 0
	for i := 0; i < size; i += 1 {
		switch t2 := t[i].(type) {
		case map[string]interface{}:
			t[i] = createPattern(t2, true)
		case []interface{}:
			if !expr {
				return createArray(t2, true)
			} else {
				v += 1
				t[i] = V{}
			}
		default:
			t[i] = V{}
			v += 1
		}
	}
	if v == size {
		return V{}
	}
	return t
}

// Why create a new DeepEqual method? Why not use reflect.DeepEqual? The reflect package is scary. Not in
// the "I don't know how to use this" way but in a "reflection is great, but unnecessary here" kind of way. There is
// no reason to use a cannon to kill this particular mosquito since we're only doing checks against objects, arrays,
// and a single empty struct V{}. See the benchmark in pattern_test.go for more justification.
func deepEqual(ax, bx map[string]interface{}) bool {
	if len(ax) != len(bx) {
		return false
	}
	var f func(a, b interface{}) bool
	f = func(a, b interface{}) bool {
		switch t := a.(type) {
		case map[string]interface{}:
			if s, ok := b.(map[string]interface{}); !ok {
				return false
			} else if !deepEqual(t, s) {
				return false
			}
			return true
		case []interface{}:
			s, ok := b.([]interface{})
			if !ok || len(s) != len(t) {
				return false
			}
			l := len(s)
			for i := 0; i < l; i += 1 {
				if !f(t[i], s[i]) {
					return false
				}
			}
			return true
		case V:
			if _, ok := b.(V); !ok {
				return false
			}
			return true
		default:
			panic(fmt.Sprintf("unexpected type %T in pattern", t))
		}
	}
	for key := range ax {
		if _, ok := bx[key]; !ok || !f(ax[key], bx[key]) {
			return false
		}
	}
	return true // len(a) == len(b) == 0
}

// A custom sorting algorithm to keep keys starting with _ before $, and $
// before everything else.
type keySorter []string

func (k keySorter) Len() int {
	return len(k)
}

func (k keySorter) Less(i, j int) bool {
	a := k[i]
	b := k[j]
	la, lb := len(a), len(b)
	if la == 0 || lb == 0 {
		if strings.Compare(a, b) < 0 {
			return true
		} else {
			return false
		}
	}
	if (a[0] == '_' && b[0] != '_') || (a[0] == '$' && b[0] != '$') {
		return true
	} else if (a[0] != '_' && b[0] == '_') || (a[0] != '$' && b[0] == '$') {
		return false
	} else if (a[0] == '_' && b[0] == '_') || (a[0] == '$' && b[0] == '$') {
		if a[1:] < b[1:] {
			return true
		} else {
			return false
		}
	} else if a < b {
		return true
	}
	return false
}

func (k keySorter) Swap(i, j int) {
	c := k[i]
	k[i] = k[j]
	k[j] = c
}
