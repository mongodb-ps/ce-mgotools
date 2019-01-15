package mongo

import (
	"bytes"
	"fmt"
	"sort"

	"mgotools/mongo/sorter"
	"mgotools/util"
)

type Pattern struct {
	pattern     map[string]interface{}
	initialized bool
}

type V struct{}

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

func compress(c interface{}) interface{} {
	switch t := c.(type) {
	case map[string]interface{}:
		for key := range t {
			if !util.ArrayInsensitiveMatchString(OPERATORS_COMPARISON, key) {
				return t
			}
		}

	case []interface{}:
		for _, value := range t {
			if _, ok := value.(V); !ok {
				return t
			}
		}

	default:
		panic("attempted to compress unexpected value")
	}

	return V{}
}

func createPattern(s map[string]interface{}, expr bool) map[string]interface{} {
	for key := range s {
		switch t := s[key].(type) {
		case map[string]interface{}:
			if !expr || util.ArrayInsensitiveMatchString(OPERATORS_COMPARISON, key) {
				s[key] = compress(createPattern(t, true))
			} else if util.ArrayInsensitiveMatchString(OPERATORS_EXPRESSION, key) {
				s[key] = createPattern(t, false)
			} else if util.ArrayInsensitiveMatchString(OPERATORS_LOGICAL, key) {
				s[key] = createPattern(t, false)
			} else {
				s[key] = V{}
			}

		case []interface{}:
			if util.ArrayInsensitiveMatchString(OPERATORS_LOGICAL, key) {
				r := sorter.Patternize(createArray(t, false))
				sort.Sort(r)
				s[key] = r.Interface()
			} else if util.ArrayInsensitiveMatchString(OPERATORS_EXPRESSION, key) {
				s[key] = compress(createArray(t, true))
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

		// Iterating over a map will happen in a randomized order. Keys must
		// be sorted and iterated in a specific order:
		// https://codereview.appspot.com/5285042/patch/9001/10003
		keys := make(sorter.Key, 0)
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

func createArray(t []interface{}, expr bool) []interface{} {
	for i := 0; i < len(t); i += 1 {
		switch t2 := t[i].(type) {
		case map[string]interface{}:
			t[i] = createPattern(t2, true)
		case []interface{}:
			if !expr {
				return createArray(t2, true)
			} else {
				t[i] = V{}
			}
		default:
			t[i] = V{}
		}
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
