package cmd

import (
	"fmt"
	"strings"
	"time"

	"mgotools/cmd/factory"
	"mgotools/mongo"
	"mgotools/parser"
	"mgotools/parser/context"
	"mgotools/record"
	"mgotools/util"

	"github.com/pkg/errors"
)

type filterCommand struct {
	factory.BaseOptions
	Log map[int]filterLog
}

type filterCommandOptions struct {
	argCount int

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
	PatternFilter            mongo.Pattern
	SeverityFilter           record.Severity
	ShortenOutput            int
	SlowerFilter             time.Duration
	TableScanFilter          bool
	TimezoneModifier         time.Duration
	ToFilter                 time.Time
	WordFilter               string
}

type filterLog struct {
	*context.Log

	argsBool       map[string]bool
	argsInt        map[string]int
	argsString     map[string]string
	commandOptions filterCommandOptions

	ErrorCount uint
	LineCount  uint
}

func init() {
	args := factory.CommandDefinition{
		Usage: "filters a log file",
		Flags: []factory.CommandArgument{
			{Name: "command", Type: factory.String, Usage: "only output log lines which are `COMMAND` of a given type. Examples: \"distinct\", \"isMaster\", \"replSetGetStatus\""},
			{Name: "component", ShortName: "c", Type: factory.String, Usage: "find all lines matching `COMPONENT`"},
			{Name: "context", Type: factory.StringFileSlice, Usage: "find all lines matching `CONTEXT`"},
			{Name: "connection", ShortName: "x", Type: factory.Int, Usage: "find all lines identified as part of `CONNECTION`"},
			{Name: "exclude", Type: factory.Bool, Usage: "exclude matching lines rather than including them"},
			{Name: "fast", Type: factory.Int, Usage: "returns only operations faster than `FAST` milliseconds"},
			{Name: "from", ShortName: "f", Type: factory.StringFileSlice, Usage: "ignore all entries before `DATE` (see help for date formatting)"},
			{Name: "marker", Type: factory.StringFileSlice, Usage: "append a pre-defined marker (filename, enum, alpha, none) or custom marker (one per file) identifying the source file of each line"},
			{Name: "message", Type: factory.Bool, Usage: "excludes all non-message portions of each line"},
			{Name: "namespace", Type: factory.String, Usage: "filter by `NAMESPACE` so only lines matching the namespace will be returned"},
			{Name: "pattern", ShortName: "p", Type: factory.String, Usage: "filter queries of shape `PATTERN` (only applies to queries, getmores, updates, removed)"},
			{Name: "severity", ShortName: "i", Type: factory.String, Usage: "find all lines of `SEVERITY`"},
			{Name: "shorten", Type: factory.Int, Usage: "reduces output by truncating log lines to `LENGTH` characters"},
			{Name: "slow", Type: factory.Int, Usage: "returns only operations slower than `SLOW` milliseconds"},
			{Name: "timezone", Type: factory.IntFileSlice, Usage: "timezone adjustment: add `N` minutes to the corresponding log file"},
			{Name: "to", ShortName: "t", Type: factory.StringFileSlice, Usage: "ignore all entries after `DATE` (see help for date formatting)"},
			{Name: "word", Type: factory.StringFileSlice, Usage: "only output lines matching `WORD`"},
		},
	}
	init := func() (factory.Command, error) {
		return &filterCommand{
			Log: make(map[int]filterLog),
		}, nil
	}
	factory.GetCommandFactory().Register("filter", args, init)
}

func (f *filterCommand) Finish(index int) error {
	// There are no operations that need to be performed when a file finishes.
	return nil
}

func (f *filterCommand) Terminate(out chan<- string) error {
	// Finish any
	return nil
}

func (f *filterCommand) ProcessLine(index int, out chan<- string, in <-chan string, errs chan<- error, fatal <-chan struct{}) error {
	accumulator := make(chan record.AccumulatorResult)

	exit := 0
	options := f.Log[index].commandOptions

	go func() {
		<-fatal
		exit = 1
	}()

	// The accumulator is designed solve the multi-line problem. The log format
	// does not escape multiple lines properly, causing weird problems when
	// parsing multi-line queries. The accumulator takes in one or more
	// record.Base objects and outputs a single record.Base object for every
	// line.
	go record.Accumulator(in, accumulator, record.NewBase)

	// Iterate through every record.Base object provided. This is identical
	// to iterating through every line of a log without multi-line queries.
	for base := range accumulator {
		if exit != 0 {
			// Received an exit signal so immediately exit.
			break
		} else if base.Error != nil {

		}

		log := f.Log[index]
		entry, err := f.Log[index].NewEntry(base.Base)

		if err != nil {
			log.ErrorCount += 1
			if _, ok := err.(parser.VersionErrorUnmatched); ok {
				errs <- err
				continue
			} else if log.commandOptions.argCount > 0 {
				continue
			}
		}

		var line string
		if entry, modified := f.modify(entry, options); modified {
			line = entry.String()
		} else {
			line = base.Base.String()
		}

		if ok := f.match(entry, f.Log[index].commandOptions); (options.InvertMatch && ok) || (!options.InvertMatch && !ok) {
			// DEBUG:
			if len(line) > 70 {
				line = line[:70]
			}
			util.Debug("*: %s", line)
			continue
		}

		if options.MessageOutput {
			line = entry.RawMessage
		}

		if options.MarkerOutput != "" {
			line = options.MarkerOutput + line
		}

		if options.ShortenOutput > 0 {
			line = entry.Prefix(options.ShortenOutput)
		}

		out <- line
	}
	return nil
}

func (f *filterCommand) Prepare(inputContext factory.InputContext) error {
	opts := filterCommandOptions{
		ConnectionFilter:         -1,
		ExecutionDurationMinimum: -1,
		InvertMatch:              false,
		JsonOutput:               false,
		MarkerOutput:             "",
		MessageOutput:            false,
		ShortenOutput:            0,
		TableScanFilter:          false,
		argCount:                 len(inputContext.Booleans) + len(inputContext.Integers) + len(inputContext.Strings),
	}
	util.Debug("Options: %+v %+v %+v", inputContext.Booleans, inputContext.Integers, inputContext.Strings)
	dateParser := util.NewDateParser([]string{
		"2006",
		"2006-01-02",
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
	for key, value := range inputContext.Booleans {
		switch key {
		case "exclude":
			opts.InvertMatch = value
		case "message":
			opts.MessageOutput = value
		}
	}
	// parse through all integer arguments
	for key, value := range inputContext.Integers {
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
	for key, value := range inputContext.Strings {
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
				opts.MarkerOutput = fmt.Sprintf("%d ", inputContext.Index)
			} else if value == "alpha" {
				for i := 0; i < inputContext.Index/26+1; i += 1 {
					opts.MarkerOutput = opts.MarkerOutput + string(rune(inputContext.Index%26+97))
				}
				opts.MarkerOutput = opts.MarkerOutput + " "
			} else if value == "filename" {
				opts.MarkerOutput = inputContext.Name + " "
			} else {
				opts.MarkerOutput = value + " "
			}
		case "namespace":
			opts.NamespaceFilter = value
		case "pattern":
			if pattern, err := mongo.ParseJson(value, false); err != nil {
				return fmt.Errorf("unrecognized pattern (%s)", err)
			} else if opts.PatternFilter = mongo.NewPattern(pattern); err != nil {
				return fmt.Errorf("failed to transform pattern")
			} else {
				util.Debug("argument pattern: %+v", opts.PatternFilter)
			}
		case "operation":
			opts.OperationFilter = value
		case "severity":
			if !util.ArgumentMatchOptions(mongo.SEVERITIES, value) {
				return errors.New("--severity is not a recognized severity")
			}
			opts.SeverityFilter = record.Severity(value[0])
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
	f.Log[inputContext.Index] = filterLog{
		Log:            context.NewLog(),
		argsBool:       inputContext.Booleans,
		argsInt:        inputContext.Integers,
		argsString:     inputContext.Strings,
		commandOptions: opts,
	}
	return nil
}

func (f *filterCommand) Usage() string {
	return "used to filter log files based on a set of criteria"
}

func (f *filterCommand) match(entry record.Entry, opts filterCommandOptions) bool {
	if opts.argCount == 0 {
		return true
	} else if !entry.Valid {
		return false
	} else if opts.ConnectionFilter > -1 && entry.Connection != opts.ConnectionFilter {
		return false
	} else if opts.ComponentFilter != "" && !stringMatchFields(entry.RawComponent, opts.ComponentFilter) {
		return false
	} else if opts.ContextFilter != "" && !stringMatchFields(entry.Context, opts.ContextFilter) {
		return false
	} else if opts.SeverityFilter > 0 && entry.RawSeverity != opts.SeverityFilter {
		return false
	} else if !entry.DateValid || (!opts.FromFilter.IsZero() && opts.FromFilter.After(entry.Date)) || (!opts.ToFilter.IsZero() && opts.ToFilter.Before(entry.Date)) {
		return false
	} else if opts.WordFilter != "" && strings.Contains(entry.String(), opts.WordFilter) {

	} else if entry.Message == nil && (opts.FasterFilter > 0 ||
		opts.SlowerFilter > 0 ||
		opts.CommandFilter != "" ||
		opts.NamespaceFilter != "" ||
		!opts.PatternFilter.IsEmpty()) {
		// Return failure on any log messages that could not be parsed when filters exist that rely on parsing a
		// log message.
		return false
	} else if entry.Message == nil {
		// Return successfully on empty log messages (since the base parts of the entry should have matched). All
		// subsequent filters should check on the log message.
		return true
	}

	// Try converting into a base MsgOpCommand object and do comparisons if the filter succeeds.
	if cmd, ok := entry.Message.(record.MsgOpCommandBase); ok {
		if opts.CommandFilter != "" && !stringMatchFields(cmd.Name, opts.CommandFilter) {
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
	// Try convergint to a MsgOpCommandLegacy object and compare filters based on that object type.
	if cmd, ok := entry.Message.(record.MsgOpCommandLegacy); ok {
		if opts.NamespaceFilter != "" && !stringMatchFields(cmd.Namespace, opts.NamespaceFilter) {
			return false
		} else if !opts.PatternFilter.IsEmpty() && !checkQueryPattern(cmd.Operation, cmd.Command, opts.PatternFilter) {
			return false
		}
	} else if cmd, ok := entry.Message.(record.MsgOpCommand); ok {
		if opts.NamespaceFilter != "" && !stringMatchFields(cmd.Namespace, opts.NamespaceFilter) {
			return false
		} else if !opts.PatternFilter.IsEmpty() && !checkQueryPattern(cmd.Operation, cmd.Command, opts.PatternFilter) {
			return false
		}
	} else if !ok && !opts.PatternFilter.IsEmpty() {
		return false
	}

	return true
}

func (f *filterCommand) modify(entry record.Entry, options filterCommandOptions) (record.Entry, bool) {
	if options.TimezoneModifier != 0 && entry.DateValid {
		// add seconds to the parsed date object
		entry.Date = entry.Date.Add(options.TimezoneModifier * time.Minute)
		return entry, true
	}
	return entry, false
}

func checkQueryPattern(op string, cmd mongo.Object, check mongo.Pattern) bool {
	for _, key := range []string{"query", "command", "update", "remove"} {
		if query, ok := cmd[key].(map[string]interface{}); ok {
			if check.Equals(mongo.NewPattern(query)) {
				return true
			}
		}
	}
	return false
}

func stringMatchFields(value string, check string) bool {
	for _, item := range strings.FieldsFunc(check, util.ArgumentSplit) {
		if util.StringInsensitiveMatch(item, value) {
			return true
		}
	}
	return false
}
