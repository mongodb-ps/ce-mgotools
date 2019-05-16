package internal

import (
	"errors"
	"regexp"
	"sync"
)

type regexRegistry struct {
	Compiled sync.Map
	mutex    sync.Mutex
}

var regexRegistryInstance = &regexRegistry{}

func GetRegexRegistry() *regexRegistry {
	return regexRegistryInstance
}

func (r *regexRegistry) Compile(pattern string) (*regexp.Regexp, error) {
	var regex interface{}
	var ok bool

	if regex, ok = r.Compiled.Load(pattern); !ok {
		r.mutex.Lock()
		defer r.mutex.Unlock()

		if compiled, err := regexp.Compile(pattern); err != nil {
			return nil, err
		} else {
			r.Compiled.Store(pattern, compiled)
			regex = compiled
		}
	}
	return regex.(*regexp.Regexp), nil
}

func (r *regexRegistry) CompileAndMatch(pattern string, match string) ([]string, error) {
	compiled, err := r.Compile(pattern)
	if err != nil {
		return nil, err
	}

	results := compiled.FindStringSubmatch(match)
	if results == nil {
		return results, errors.New("empty match")
	}

	return results, nil
}
