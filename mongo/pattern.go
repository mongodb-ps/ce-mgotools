package mongo

import (
	"bytes"
	"fmt"
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
func (p *Pattern) Pattern() Object {
	return p.pattern
}
func (p *Pattern) String() string {
	if !p.initialized {
		return ""
	}
	var arr func(Array) string
	var obj func(Object) string
	arr = func(array Array) string {
		var buffer = bytes.NewBufferString("[")
		total := len(array)
		if total > 0 {
			buffer.WriteRune(' ')
		}
		for index := 0; index < total; index += 1 {
			switch t := array[index].(type) {
			case Array:
				buffer.WriteString(arr(t))
			case Object:
				buffer.WriteString(obj(t))
			case V:
				buffer.WriteRune('1')
			}
			if index < total-1 {
				buffer.WriteString(", ")
			} else {
				buffer.WriteRune(' ')
			}
		}
		buffer.WriteRune(']')
		return buffer.String()
	}
	obj = func(object Object) string {
		var buffer = bytes.NewBufferString("{")
		total := len(object)
		count := 0
		if total > 0 {
			buffer.WriteRune(' ')
		}
		for key := range object {
			count += 1
			buffer.WriteString(key)
			buffer.WriteString(": ")
			switch t := object[key].(type) {
			case Array:
				buffer.WriteString(arr(t))
			case Object:
				buffer.WriteString(obj(t))
			case V:
				buffer.WriteRune('1')
			}
			if count < total {
				buffer.WriteString(", ")
			} else {
				buffer.WriteRune(' ')
			}
		}
		buffer.WriteString("}")
		return buffer.String()
	}
	return obj(p.pattern)
}
func createPattern(s Object, expr bool) Object {
	var arr func(Array, bool) Array
	arr = func(t Array, expr bool) Array {
		size := len(t)
		for i := 0; i < size; i += 1 {
			switch t2 := t[i].(type) {
			case Object:
				t[i] = createPattern(t2, true)
			case Array:
				if !expr {
					t[i] = arr(t2, true)
				} else {
					t[i] = V{}
				}
			default:
				t[i] = V{}
			}
		}
		return t
	}
	for key := range s {
		switch t := s[key].(type) {
		case Object:
			if !expr || util.ArrayInsensitiveMatchString(OPERATORS_COMPARISON, key) {
				s[key] = createPattern(t, true)
			} else if util.ArrayInsensitiveMatchString(OPERATORS_LOGICAL, key) || util.ArrayInsensitiveMatchString(OPERATORS_EXPRESSION, key) {
				s[key] = createPattern(t, false)
			} else {
				s[key] = V{}
			}
		case Array:
			if util.ArrayInsensitiveMatchString(OPERATORS_LOGICAL, key) {
				s[key] = arr(t, false)
			} else if util.ArrayInsensitiveMatchString(OPERATORS_EXPRESSION, key) {
				s[key] = arr(t, true)
			} else {
				s[key] = V{}
			}
		default:
			s[key] = V{}
		}
	}
	return s
}

// Why create a new DeepEqual method? Why not use reflect.DeepEqual? The reflect package is scary. Not in
// the "I don't know how to use this" way but in a "reflection is great, but unnecessary here" kind of way. There is
// no reason to use a cannon to kill this particular mosquito since we're only doing checks against objects, arrays,
// and a single empty struct V{}. See the benchmark in pattern_test.go for more justification.
func deepEqual(ax, bx Object) bool {
	if len(ax) != len(bx) {
		return false
	}
	var f func(a, b interface{}) bool
	f = func(a, b interface{}) bool {
		switch t := a.(type) {
		case Object:
			if s, ok := b.(Object); !ok {
				return false
			} else if !deepEqual(t, s) {
				return false
			}
			return true
		case Array:
			s, ok := b.(Array)
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
			panic(fmt.Sprintf("unexpected type in pattern: %#v", a))
		}
	}
	for key := range ax {
		if _, ok := bx[key]; !ok || !f(ax[key], bx[key]) {
			return false
		}
	}
	return true // len(a) == len(b) == 0
}
