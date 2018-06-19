package cmd

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"

	"mgotools/cmd/factory"
	"mgotools/cmd/format"
	"mgotools/mongo"
	"mgotools/parser"
	"mgotools/parser/context"
	"mgotools/record"
	"mgotools/util"
)

type commandQuery struct {
	*context.Log
	Name   string
	Length int64

	sort string

	ErrorCount uint
	LineCount  uint

	Patterns map[string]queryPattern
}

type queryLog struct {
	//factory.BaseOptions
	Log          map[int]*commandQuery
	summaryTable *bytes.Buffer
}

type queryPattern struct {
	format.PatternSummary

	n95Sequence float64
	sync        sync.Mutex
}

func init() {
	args := factory.CommandDefinition{
		Usage: "output statistics about query patterns",
		Flags: []factory.CommandArgument{
			{Name: "sort", ShortName: "s", Type: factory.String, Usage: "sort by namespace, pattern, count, min, max, 95%, or sum"},
		},
	}
	init := func() (factory.Command, error) {
		return &queryLog{Log: make(map[int]*commandQuery), summaryTable: bytes.NewBuffer([]byte{})}, nil
	}
	factory.GetCommandFactory().Register("query", args, init)
}

func (s *queryLog) Finish(index int) error {
	var host string
	var port int

	for _, startup := range s.Log[index].Startup {
		host = startup.Hostname
		port = startup.Port
	}

	summary := format.LogSummary{
		Source:     s.Log[index].Name,
		Host:       host,
		Port:       port,
		Start:      s.Log[index].Start,
		End:        s.Log[index].End,
		DateFormat: "",
		Length:     s.Log[index].Length,
		Version:    nil,
		Storage:    "",
	}

	values := make([]format.PatternSummary, 0, len(s.Log))

	for _, pattern := range s.Log[index].Patterns {
		values = append(values, pattern.PatternSummary)
	}

	var sorter sortFunction = func() (string, []format.PatternSummary) {
		return s.Log[index].sort, values
	}

	sort.Sort(sorter)

	if index > 0 {
		s.summaryTable.WriteString("\n------------------------------------------\n")
	}

	format.PrintLogSummary(summary, os.Stdout)
	format.PrintQueryTable(values, s.summaryTable)
	return nil
}

func (s *queryLog) Prepare(inputContext factory.InputContext) error {
	s.Log[inputContext.Index] = &commandQuery{
		Log:      context.NewLog(parser.VersionParserFactory.Get()),
		Name:     inputContext.Name,
		Length:   inputContext.Length,
		Patterns: make(map[string]queryPattern),

		sort: "sum",
	}

	if sortType, ok := inputContext.Strings["sort"]; ok {
		s.Log[inputContext.Index].sort = sortType
	}
	return nil
}

func (s *queryLog) ProcessLine(index int, out chan<- string, in <-chan string, errors chan<- error, halt <-chan struct{}) error {
	exit := false

	// Wait for kill signals.
	go func() {
		<-halt
		exit = true
	}()

	accumulator := make(chan record.AccumulatorResult)
	go record.Accumulator(in, accumulator, record.NewBase)

	// A function to grab new lines and parse them.
	for base := range accumulator {
		if exit {
			util.Debug("exit signal received")
			break
		}

		log := s.Log[index]
		log.LineCount += 1

		if base.Error != nil || base.Base.RawMessage == "" {
			log.ErrorCount += 1
		} else if entry, err := log.NewEntry(base.Base); err != nil {
			log.ErrorCount += 1
		} else {
			log.End = entry.Date

			if ns, op, query, dur := group(entry); op != "" && query != "" {
				key := ns + ":" + op + ":" + query
				pattern, ok := log.Patterns[key]
				if !ok {
					pattern = queryPattern{
						PatternSummary: format.PatternSummary{
							Namespace: ns,
							Operation: op,
							Pattern:   query,
						},
						n95Sequence: 0,
					}
				}
				log.Patterns[key] = updateSummary(pattern, dur)
			}
		}
	}

	return nil
}

func (s *queryLog) Terminate(out chan<- string) error {
	out <- string(s.summaryTable.String())
	return nil
}

// A function to transform to a log entry to a pattern.
func group(entry record.Entry) (string, string, string, int64) {
	cmd, ok := record.MsgOpCommandBaseFromMessage(entry.Message)
	if !ok {
		return "", "", "", 0
	}

	op, ok := record.MsgOperationFromMessage(entry.Message)
	if !ok || op.Operation == "" {
		return "", "", "", 0
	}

	switch cmd.Command {
	case "count",
		"find",
		"getmore",
		"geonear",
		"remove",
		"distinct":

		if query, ok := parser.NormalizeQuery(cmd); ok {
			pattern := mongo.NewPattern(query)
			return op.Namespace, op.Operation, pattern.StringCompact(), cmd.Duration
		}

	case "aggregate",
		"update":

	default:
		panic(fmt.Sprintf("unexpected %s in query operation at line %d", cmd.Command, entry.LineNumber))
	}
	return op.Namespace, op.Operation, "None", cmd.Duration
}

func updateSummary(s queryPattern, dur int64) queryPattern {
	s.Count += 1
	s.Sum += dur
	if dur > s.Max {
		s.Max = dur
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
	return s
}

type sortFunction func() (string, []format.PatternSummary)

func (s sortFunction) Len() int {
	_, v := s()
	return len(v)
}

func (s sortFunction) Less(i, j int) bool {
	field, v := s()

	switch field {
	case "namespace":
		return v[i].Namespace < v[j].Namespace // Ascending
	case "pattern":
		return v[i].Pattern < v[j].Pattern // Ascending
	case "count":
		return v[i].Count > v[j].Count // Descending
	case "min":
		return v[i].Min < v[j].Min // Ascending
	case "max":
		return v[i].Max > v[j].Max // Descending
	case "mean":
		return (v[i].Count / v[i].Sum) > (v[j].Count / v[j].Sum) // Descending
	case "95%":
		return v[i].N95Percentile > v[j].N95Percentile // Descending
	default:
		return v[i].Sum > v[j].Sum // Descending
	}
}

func (s sortFunction) Swap(i, j int) {
	_, v := s()
	t := v[i]
	v[i] = v[j]
	v[j] = t
}
