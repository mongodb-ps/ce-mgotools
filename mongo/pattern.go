package mongo

import (
	"mgotools/util"
)

type Pattern struct {
	Pattern map[string]interface{}
}
type V struct {
}

func NewPattern(s map[string]interface{}) (*Pattern, error) {
	return &Pattern{patternize(s, false)}, nil
}

func patternize(s map[string]interface{}, expr bool) map[string]interface{} {
	for key, _ := range s {
		switch t := s[key].(type) {
		case map[string]interface{}:
			if !expr {
				s[key] = patternize(t, true)
			} else if util.ArrayInsensitiveMatchString(OPERATORS_LOGICAL, key) {
				s[key] = patternize(t, false)
			} else if util.ArrayInsensitiveMatchString(OPERATORS_COMPARISON, key) {
				s[key] = V{}
			}
		case []interface{}:
			s[key] = patternizeArray(t)
		default:
			s[key] = V{}
		}
	}
	return s
}
func patternizeArray(t []interface{}) []interface{} {
	size := len(t)
	for i := 0; i < size; i += 1 {
		switch t2 := t[i].(type) {
		case map[string]interface{}:
			t[i] = patternize(t2, true)
		case []interface{}:
			t[i] = patternizeArray(t)
		default:
			t[i] = V{}
		}
	}
	return t
}
