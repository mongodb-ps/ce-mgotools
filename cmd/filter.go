package cmd

import (
	"fmt"
	"github.com/pkg/errors"
	"mgotools/mongo"
	"mgotools/parser"
	"mgotools/util"
	"strings"
	"time"
)

type filterCommand struct {
	BaseOptions
	Log map[int]filterLog
}
type filterCommandOptions struct {
	AppNameFilter            string
	CommandFilter            string
	ComponentFilter          string
	ConnectionFilter         int
	ContextFilter            string
	ExecutionDurationMinimum int
	FasterFilter             time.Duration
	FromFilter               time.Time
	InvertMatch              bool
	JsonOutput               bool
	MarkerOutput             string
	MessageOutput            bool
	NamespaceFilter          string
	OperationFilter          string
	PatternFilter            string
	SeverityFilter           string
	ShortenOutput            int
	SlowerFilter             time.Duration
	TableScanFilter          bool
	TimezoneModifier         time.Duration
	ToFilter                 time.Time
	WordFilter               string

	argCount   int
	errorCount int
	lineCount  int
}
type filterLog struct {
	parser.LogEntryFactory
	argsBool       map[string]bool
	argsInt        map[string]int
	argsString     map[string]string
	commandOptions filterCommandOptions
}

func (f *filterCommand) Finish(out chan<- string) error {
	//out <- fmt.Sprintf("final counts: %d lines, %d errors", f.lineCount, f.errorCount)
	return nil
}
func (f *filterCommand) ProcessLine(index int, out chan<- string, in <-chan string, errs chan<- error, fatal <-chan struct{}) error {
	var (
		options filterCommandOptions = f.Log[index].commandOptions
		exit    int                  = 0
	)
	go func() {
		<-fatal
		exit = 1
	}()
	for line := range in {
		switch {
		case exit != 0:
			// Received an exit signal so immediately exit.
			fmt.Println("Received exit signal")
			return nil
		case line == "":
		default:
			options.lineCount += 1
			raw, err := f.Log[index].NewRawLogEntry(line)
			if err != nil {
				panic(err)
			}
			entry, err := f.Log[index].NewLogEntry(raw)
			if err != nil {
				if _, ok := err.(parser.LogVersionErrorUnmatched); !ok {
					errs <- err
					options.errorCount += 1
				}
			} else {
				if entry, modified := f.modify(entry, options); modified {
					line = entry.String()
				}
				if ok := f.match(entry, f.Log[index].commandOptions); (!options.InvertMatch && ok) || (options.InvertMatch && !ok) {
					if options.ShortenOutput > 0 {
						line = entry.Prefix(options.ShortenOutput)
					} else if options.MessageOutput {
						line = entry.RawMessage
					}
					if options.MarkerOutput != "" {
						out <- options.MarkerOutput + line
					} else {
						out <- line
					}
				}
			}
		}
	}
	return nil
}
func (f *filterCommand) Init() {
	f.Log = make(map[int]filterLog)
}
func (f *filterCommand) Prepare(name string, index int, factory parser.LogEntryFactory, booleans map[string]bool, integers map[string]int, strings map[string]string) error {
	opts := filterCommandOptions{
		ConnectionFilter:         -1,
		ExecutionDurationMinimum: -1,
		InvertMatch:              false,
		JsonOutput:               false,
		MarkerOutput:             "",
		MessageOutput:            false,
		ShortenOutput:            0,
		TableScanFilter:          false,
		argCount:                 len(booleans) + len(integers) + len(strings),
	}
	util.Debug("Options: %+v %+v %+v", booleans, integers, strings)
	dateParser := util.NewDateParser([]string{
		"2006",
		"2016-01-02",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05-0700",
		"2006-01-02T15:04:05 MST",
		"2006-01-02T15:04:05.000",
		"2006-01-02T15:04:05.000-0700",
		"2006-01-02T15:04:05.000 MST",
		"15:04:05",
		"15:04:05.000",
		"15:04:05-0700",
		"15:04:05.000-0700",
		"15:04:05 MST",
		"15:04:05.000 MST",
		"Mon Jan 2 15:04:05",
		"Mon Jan 2 15:04:05-0700",
		"Mon Jan 2 15:04:05 MST",
		"Mon Jan 2 15:04:05.000",
		"Mon Jan 2 15:04:05.000-0700",
		"Mon Jan 2 15:04:05.000 MST",
		"Jan 2",
		"Jan 2 2006",
		"Jan 2 2006 15:04:05",
		"Jan 2 2006 15:04:05.000",
		"Jan 2 2006 15:04:05-0700",
		"Jan 2 2006 15:04:05.000-0700",
		"Jan 2 2006 15:04:05 MST",
		"Jan 2 2006 15:04:05.000 MST",
	})
	// parse through all boolean arguments
	for key, value := range booleans {
		switch key {
		case "exclude":
			opts.InvertMatch = value
		case "message":
			opts.MessageOutput = value
		}
	}
	// parse through all integer arguments
	for key, value := range integers {
		switch key {
		case "connection":
			if value > 0 {
				opts.ConnectionFilter = value
			}
		case "fast":
			if value < 0 {
				return errors.New("--fast must be greater than 0ms")
			}
			opts.FasterFilter = time.Duration(value)
		case "shorten":
			if value < 10 {
				return errors.New("--shorten must be longer than 10 characters")
			}
			opts.ShortenOutput = value
		case "slow":
			if value < 1 {
				return errors.New("--slow must be greater than 0ms")
			}
			opts.SlowerFilter = time.Duration(value)
		case "timezone":
			if value < -719 || value > 720 {
				return errors.New("--timezone must be an offset between -719 and +720 minutes")
			}
			opts.TimezoneModifier = time.Duration(value)
		}
	}
	// parse through all string arguments
	for key, value := range strings {
		if value == "" {
			return fmt.Errorf("%s cannot be empty", key)
		}
		switch key {
		case "appname":
			opts.AppNameFilter = value
		case "command":
			opts.CommandFilter = value
		case "component":
			if !util.ArgumentMatchOptions(mongo.COMPONENTS, value) {
				return errors.New("--component is not a recognized component")
			}
			opts.ComponentFilter = value
		case "context":
			opts.ContextFilter = value
		case "from":
			if dateParser, err := dateParser.ParseDate(value); err != nil {
				return errors.New("--from flag could not be parsed")
			} else {
				opts.FromFilter = dateParser
				fmt.Println(fmt.Sprintf("From: %s", dateParser.String()))
			}
		case "marker":
			if value == "enum" {
				opts.MarkerOutput = fmt.Sprintf("%d ", index)
			} else if value == "alpha" {
				for i := 0; i < index/26+1; i += 1 {
					opts.MarkerOutput = opts.MarkerOutput + string(rune(index%26+97))
				}
				opts.MarkerOutput = opts.MarkerOutput + " "
			} else if value == "filename" {
				opts.MarkerOutput = name + " "
			} else {
				opts.MarkerOutput = value + " "
			}
		case "namespace":
			opts.NamespaceFilter = value
		case "operation":
			opts.OperationFilter = value
		case "severity":
			if !util.ArgumentMatchOptions(mongo.SEVERITIES, value) {
				return errors.New("--severity is not a recognized severity")
			}
			opts.SeverityFilter = value
		case "to":
			if dateParser, err := dateParser.ParseDate(value); err != nil {
				return errors.New("--to flag could not be parsed")
			} else {
				opts.ToFilter = dateParser
				fmt.Println(fmt.Sprintf("To: %s", dateParser.String()))
			}
		case "word":
			if value != "" {
				opts.WordFilter = value
			}
		}
	}
	f.Log[index] = filterLog{
		LogEntryFactory: factory,
		argsBool:        booleans,
		argsInt:         integers,
		argsString:      strings,
		commandOptions:  opts,
	}
	return nil
}
func (f *filterCommand) Usage() string {
	return "used to filter log files based on a set of criteria"
}
func (f *filterCommand) match(entry parser.LogEntry, opts filterCommandOptions) bool {
	if !entry.Valid {
		return false
	} else if opts.argCount == 0 {
		return true
	} else if opts.ConnectionFilter > -1 && entry.Connection != opts.ConnectionFilter {
		return false
	} else if opts.ComponentFilter != "" && !stringMatchFields(entry.RawComponent, opts.ComponentFilter) {
		return false
	} else if opts.ContextFilter != "" && !stringMatchFields(entry.Context, opts.ContextFilter) {
		return false
	} else if opts.SeverityFilter != "" && !stringMatchFields(entry.RawSeverity, opts.SeverityFilter) {
		return false
	} else if !entry.DateValid || (!opts.FromFilter.IsZero() && opts.FromFilter.After(entry.Date)) || (!opts.ToFilter.IsZero() && opts.ToFilter.Before(entry.Date)) {
		return false
	} else if opts.WordFilter != "" && strings.Contains(entry.String(), opts.WordFilter) {

	} else if entry.LogMessage == nil && (opts.FasterFilter > 0 || opts.SlowerFilter > 0 || opts.CommandFilter != "" || opts.NamespaceFilter != "") {
		// Return failure on any log messages that could not be parsed when filters exist that rely on parsing a
		// log message.
		return false
	} else if entry.LogMessage == nil {
		// Return successfully on empty log messages (since the raw parts of the entry should have matched). All
		// subsequent filters should check on the log message.
		return true
	}

	// Try converting into a base LogMsgOpCommand object and do comparisons if the filter succeeds.
	if cmd, ok := entry.LogMessage.(parser.LogMsgOpCommand); ok {
		if opts.NamespaceFilter != "" && !stringMatchFields(cmd.Namespace, opts.NamespaceFilter) {
			return false
		} else if opts.CommandFilter != "" && !stringMatchFields(cmd.Name, opts.CommandFilter) {
			return false
		} else if opts.FasterFilter > 0 && time.Duration(cmd.Duration) > opts.FasterFilter {
			return false
		} else if opts.SlowerFilter > 0 && time.Duration(cmd.Duration) < opts.SlowerFilter {
			return false
		}
	} else if opts.FasterFilter > 0 || opts.SlowerFilter > 0 || opts.CommandFilter != "" || opts.NamespaceFilter != "" {
		// Return failure on any log messages that are parsed successfully but don't contain components that can be
		// filtered based on command-style criteria.
		return false
	}

	return true
}
func (f *filterCommand) modify(entry parser.LogEntry, options filterCommandOptions) (parser.LogEntry, bool) {
	if options.TimezoneModifier != 0 && entry.DateValid {
		// add seconds to the parsed date object
		entry.Date = entry.Date.Add(options.TimezoneModifier * time.Minute)
		return entry, true
	}
	return entry, false
}
func stringMatchFields(value string, check string) bool {
	for _, item := range strings.FieldsFunc(check, util.ArgumentSplit) {
		if util.StringInsensitiveMatch(item, value) {
			return true
		}
	}
	return false
}
