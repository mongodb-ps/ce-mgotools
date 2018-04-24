package cmd

import (
	"math"
	"sync"

	"mgotools/cmd/factory"
	"mgotools/mongo"
	"mgotools/parser/context"
	"mgotools/record"
	"mgotools/util"
)

type commandQuery struct {
	Name string

	ErrorCount uint
	LineCount  uint

	Log      *context.Log
	Patterns map[string]*queryPattern
}
type queryLog struct {
	//factory.BaseOptions
	Log map[int]commandQuery
}
type queryPattern struct {
	Pattern       string
	Operation     string
	Count         int64
	Min           int64
	Max           int64
	N95Percentile float64
	Sum           int64

	n95Sequence float64
	sync        sync.Mutex
}

func init() {
	args := factory.CommandDefinition{}
	init := func() (factory.Command, error) {
		return &queryLog{Log: make(map[int]commandQuery)}, nil
	}
	factory.GetCommandFactory().Register("query", args, init)
}

func (s *queryLog) Finish(index int) error {
	return nil
}
func (s *queryLog) Prepare(inputContext factory.InputContext) error {
	s.Log[inputContext.Index] = commandQuery{
		inputContext.Name,
		0,
		0,
		context.NewLog(),
		make(map[string]*queryPattern),
	}
	return nil
}
func (s *queryLog) ProcessLine(index int, out chan<- string, in <-chan string, errors chan<- error, fatal <-chan struct{}) error {
	var (
		exit bool = false
		lock sync.Mutex
	)
	wg := sync.WaitGroup{}
	// Wait for kill signals.
	go func() {
		<-fatal
		exit = true
	}()

	// A function to transform to a log entry to a pattern.
	queries := func(entry record.Entry) (string, string, int64, bool) {
		cmd := entry.Message
		if cmd == nil {
			return "", "", 0, false
		}
		switch t := cmd.(type) {
		case record.MsgOpCommand:
			if query, ok := t.Command["query"].(mongo.Object); ok {
				pattern := mongo.NewPattern(query)
				return t.Name, pattern.String(), t.Duration, true
			}
		case record.MsgOpCommandLegacy:
			if query, ok := t.Command["query"].(mongo.Object); ok {
				pattern := mongo.NewPattern(query)
				return t.Name, pattern.String(), t.Duration, true
			}
		default:
			return "", "", 0, true
		}
		return "", "", 0, false
	}

	// A function to get a pattern reference in the set of possible patterns.
	getpattern := func(op, query string, patterns map[string]*queryPattern) *queryPattern {
		key := op + ":" + query
		for {
			if pattern, ok := patterns[key]; !ok {
				lock.Lock()
				if _, ok := patterns[key]; ok {
					continue
				}
				util.Debug("adding pattern: %s", key)
				patterns[key] = &queryPattern{
					Operation: op,
					Pattern:   query,
				}
				lock.Unlock()
			} else {
				return pattern
			}
		}
	}

	updatesummary := func(s *queryPattern, dur int64) {
		s.sync.Lock()
		defer s.sync.Unlock()
		s.Count += 1
		s.Sum += dur
		if dur > s.Max {
			dur = s.Max
		}
		if dur < s.Min {
			s.Min = dur
		}
		// Calculate the 95th percentile using a moving percentile estimation.
		// http://mjambon.com/2016-07-23-moving-percentile/
		s.n95Sequence = math.Pow(float64(s.Sum)/float64(s.Count)-float64(dur), 2)
		if s.Count == 1 {
			s.N95Percentile = float64(dur)
		} else if float64(dur) < s.N95Percentile {
			s.N95Percentile = s.N95Percentile - (0.005*math.Sqrt(s.n95Sequence/float64(s.Sum)))/.9
		} else if float64(dur) > s.N95Percentile {
			s.N95Percentile = s.N95Percentile + (0.005*math.Sqrt(s.n95Sequence/float64(s.Sum)))/.1
		}
		util.Debug("updating values for %s:%s: %#v", s.Operation, s.Pattern, s)
	}

	// A function to grab new lines and parse them.
	parse := func() {
		wg.Add(1)
		defer wg.Done()
		for line := range in {
			if exit {
				util.Debug("exit signal received")
				break
			} else if line == "" {
				continue
			}
			//util.Debug("%s", line)
			c := s.Log[index]
			c.LineCount += 1
			if base, err := record.NewBase(line, c.LineCount); err != nil {
				c.ErrorCount += 1
			} else if base.RawMessage == "" {
				continue
			} else if entry, err := c.Log.NewEntry(base); err != nil {
				c.ErrorCount += 1
			} else if op, query, dur, ok := queries(entry); !ok {
				c.ErrorCount += 1
			} else if op != "" && query != "" {
				updatesummary(getpattern(op, query, c.Patterns), dur)
			}
		}
	}
	for i := 0; i < 4; i += 1 {
		go parse()
	}
	// Wait for the workers to finish.
	util.Debug("starting wait in ProcessLine")
	wg.Wait()
	util.Debug("finishing wait in ProcessLine")
	return nil
}
func (s *queryLog) Terminate(out chan<- string) error {
	return nil
}
