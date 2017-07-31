package util

import (
	"regexp"
	"sync"
)

type regexRegistry struct {
	Compiled map[string]*regexp.Regexp
	Lock     sync.Mutex
}

var mutex sync.Mutex

var regexRegistryInstance *regexRegistry

func GetRegexRegistry() *regexRegistry {
	mutex.Lock()
	defer mutex.Unlock()

	if regexRegistryInstance == nil {
		regexRegistryInstance = &regexRegistry{
			Compiled: make(map[string]*regexp.Regexp),
		}
	}

	return regexRegistryInstance
}

func (r *regexRegistry) Compile(pattern string) (*regexp.Regexp, error) {
	var ok bool
	var compiled *regexp.Regexp
	var err error

	if compiled, ok = r.Compiled[pattern]; !ok {
		mutex.Lock()
		defer mutex.Unlock()

		if compiled, ok = r.Compiled[pattern]; ok {
			return compiled, nil
		}

		compiled, err = regexp.Compile(pattern)
		r.Compiled[pattern] = compiled
	}

	return compiled, err
}

func (r *regexRegistry) CompileAndMatch(pattern string, match string) ([]string, error) {
	compiled, err := r.Compile(pattern)
	if err != nil {
		return nil, err
	}

	return compiled.FindStringSubmatch(match), nil
}
