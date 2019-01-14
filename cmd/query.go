package cmd

// TODO:
//   count by namespace
//   group by IXSCAN
//   group by SORT

import (
	"bytes"
	"math"
	"os"
	"sort"
	"sync"

	"mgotools/cmd/format"
	"mgotools/mongo"
	"mgotools/parser"
	"mgotools/parser/context"
	"mgotools/record"
	"mgotools/util"

	"github.com/pkg/errors"
)

const (
	sortNamespace = iota
	sortOperation
	sortPattern
	sortCount
	sortMin
	sortMax
	sortN95
	sortSum
)

type commandQuery struct {
	*context.Instance
	Name   string
	Length uint

	sort []int8

	ErrorCount uint
	LineCount  uint

	Patterns map[string]queryPattern
}

type queryLog struct {
	Log          map[int]*commandQuery
	summaryTable *bytes.Buffer
}

type queryPattern struct {
	format.PatternSummary

	cursorId int64
	p95      []int64
	sync     sync.Mutex
}

func init() {
	args := CommandDefinition{
		Usage: "output statistics about query patterns",
		Flags: []CommandArgument{
			{Name: "sort", ShortName: "s", Type: String, Usage: "sort by namespace, pattern, count, min, max, 95%, and/or sum (comma separated for multiple)"},
		},
	}

	init := func() (Command, error) {
		return &queryLog{Log: make(map[int]*commandQuery), summaryTable: bytes.NewBuffer([]byte{})}, nil
	}

	GetCommandFactory().Register("query", args, init)
}

func (s *queryLog) Finish(index int) error {
	var host string
	var port int

	log := s.Log[index]
	for _, startup := range log.Startup {
		host = startup.Hostname
		port = startup.Port
	}

	summary := format.LogSummary{
		Source:     log.Name,
		Host:       host,
		Port:       port,
		Start:      log.Start,
		End:        log.End,
		DateFormat: "",
		Length:     int64(log.Length),
		Version:    nil,
		Storage:    "",
	}

	values := s.values(log.Patterns)
	s.sort(values, log.sort)

	if index > 0 {
		s.summaryTable.WriteString("\n------------------------------------------\n")
	}

	format.PrintLogSummary(summary, os.Stdout)
	format.PrintQueryTable(values, s.summaryTable)
	return nil
}

func (s *queryLog) Prepare(name string, instance int, args CommandArgumentCollection) error {
	s.Log[instance] = &commandQuery{
		Instance: context.NewInstance(parser.VersionParserFactory.GetAll()),
		Name:     name,
		Patterns: make(map[string]queryPattern),

		sort: []int8{sortSum, sortNamespace, sortOperation, sortPattern},
	}

	sortOptions := map[string]int8{
		"namespace": sortNamespace,
		"operation": sortOperation,
		"pattern":   sortPattern,
		"count":     sortCount,
		"min":       sortMin,
		"max":       sortMax,
		"95%":       sortN95,
		"sum":       sortSum,
	}

	for _, opt := range util.ArgumentSplit(args.Strings["sort"]) {
		val, ok := sortOptions[opt]
		if !ok {
			return errors.New("unexpected sort option")
		}
		s.Log[instance].sort = append(s.Log[instance].sort, val)
	}

	return nil
}

func (s *queryLog) Run(instance int, out commandTarget, in commandSource, errs commandError, halt commandHalt) {
	exit := false

	// Wait for kill signals.
	go func() {
		<-halt
		exit = true
	}()

	// Hold a configuration object for future use.
	log := s.Log[instance]

	// A function to grab new lines and parse them.
	for base := range in {
		if exit {
			util.Debug("exit signal received")
			break
		}

		log.LineCount += 1
		log.Length = base.LineNumber

		if base.RawMessage == "" {
			log.ErrorCount += 1
		} else if entry, err := log.NewEntry(base); err != nil {
			log.ErrorCount += 1
		} else {
			log.End = entry.Date
			crud, ok := entry.Message.(record.MsgCRUD)
			if !ok {
				// Ignore non-CRUD operations for query purposes.
				continue
			}

			pattern := mongo.NewPattern(crud.Filter)
			query := pattern.StringCompact()

			ns, op, dur, ok := s.standardize(crud)
			if !ok {
				log.ErrorCount += 1
				continue
			}

			op = util.StringToLower(op)

			switch op {
			case "find":
			case "count":
			case "update":
			case "getmore":
			case "remove":
			case "findandmodify":
			case "geonear":

			default:
				continue
			}

			if op != "" && query != "" {
				key := ns + ":" + op + ":" + query
				pattern, ok := log.Patterns[key]

				if !ok {
					pattern = queryPattern{
						PatternSummary: format.PatternSummary{
							Min:       math.MaxInt64,
							Namespace: ns,
							Operation: op,
							Pattern:   query,
						},
						p95: make([]int64, 0, 16*1024*1024),
					}
				}

				log.Patterns[key] = s.update(pattern, dur)
			}
		}
	}

	return
}

func (queryLog) sort(values []format.PatternSummary, order []int8) {
	sort.Slice(values, func(i, j int) bool {
		for _, field := range order {
			switch field {
			case sortNamespace: // Ascending
				if values[i].Namespace == values[j].Namespace {
					continue
				}
				return values[i].Namespace < values[j].Namespace
			case sortOperation: // Ascending
				if values[i].Operation == values[j].Operation {
					continue
				}
				return values[i].Operation < values[j].Operation
			case sortPattern: // Ascending
				if values[i].Pattern == values[j].Pattern {
					continue
				}
				return values[i].Pattern < values[j].Pattern
			case sortSum: // Descending
				if values[i].Sum == values[j].Sum {
					continue
				}
				return values[i].Sum >= values[j].Sum
			case sortN95: // Descending
				if values[i].N95Percentile == values[j].N95Percentile {
					continue
				}
				return values[i].N95Percentile >= values[j].N95Percentile
			case sortMax: // Descending
				if values[i].Max == values[j].Max {
					continue
				}
				return values[i].Max >= values[j].Max
			case sortMin: // Descending
				if values[i].Min == values[j].Min {
					continue
				}
				return values[i].Min >= values[j].Min
			case sortCount: // Descending
				if values[i].Count == values[j].Count {
					continue
				}
				return values[i].Count >= values[j].Count
			}
		}
		return true
	})
}

func (queryLog) standardize(crud record.MsgCRUD) (ns string, op string, dur int64, ok bool) {
	ok = true
	switch cmd := crud.Message.(type) {
	case record.MsgCommand:
		dur = cmd.Duration
		ns = cmd.Namespace
		op = cmd.Command

	case record.MsgCommandLegacy:
		dur = cmd.Duration
		ns = cmd.Namespace
		op = cmd.Command

	case record.MsgOperation:
		dur = cmd.Duration
		ns = cmd.Namespace
		op = cmd.Operation

	case record.MsgOperationLegacy:
		dur = cmd.Duration
		ns = cmd.Namespace
		op = cmd.Operation

	default:
		// Returned something completely unexpected so ignore the line.
		ok = false
	}

	return
}

func (s *queryLog) Terminate(out chan<- string) error {
	out <- string(s.summaryTable.String())
	return nil
}

func (queryLog) update(s queryPattern, dur int64) queryPattern {
	s.Count += 1
	s.Sum += dur
	s.p95 = append(s.p95, dur)

	if dur > s.Max {
		s.Max = dur
	}
	if dur < s.Min {
		s.Min = dur
	}

	return s
}

func (s *queryLog) values(patterns map[string]queryPattern) []format.PatternSummary {
	values := make([]format.PatternSummary, 0, len(s.Log))
	for _, pattern := range patterns {
		sort.Slice(pattern.p95, func(i, j int) bool { return pattern.p95[i] > pattern.p95[j] })

		if len(pattern.p95) > 1 {
			index := float64(len(pattern.p95)) * 0.05
			if math.Floor(index) == index {
				pattern.PatternSummary.N95Percentile = (float64(pattern.p95[int(index)-1] + pattern.p95[int(index)])) / 2
			} else {
				pattern.PatternSummary.N95Percentile = float64(pattern.p95[int(index)])
			}
		}

		values = append(values, pattern.PatternSummary)
	}
	return values
}
