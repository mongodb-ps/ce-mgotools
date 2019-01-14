package cmd

// TODO:
//   filter by IXSCAN (and pattern)
//   output w/o II

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"mgotools/mongo"
	"mgotools/parser"
	"mgotools/parser/context"
	"mgotools/record"
	"mgotools/util"

	parser_errors "mgotools/parser/errors"
)

type filterCommand struct {
	BaseOptions
	Instance map[int]filterInstance
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

type filterInstance struct {
	*context.Instance
	commandOptions filterCommandOptions

	ErrorCount uint
	LineCount  uint
}

func init() {
	args := CommandDefinition{
		Usage: "filters a log file",
		Flags: []CommandArgument{
			{Name: "command", Type: String, Usage: "only output log lines which are `COMMAND` of a given type. Examples: \"distinct\", \"isMaster\", \"replSetGetStatus\""},
			{Name: "component", ShortName: "c", Type: String, Usage: "find all lines matching `COMPONENT`"},
			{Name: "context", Type: StringSourceSlice, Usage: "find all lines matching `CONTEXT`"},
			{Name: "connection", ShortName: "x", Type: Int, Usage: "find all lines identified as part of `CONNECTION`"},
			{Name: "exclude", Type: Bool, Usage: "exclude matching lines rather than including them"},
			{Name: "fast", Type: Int, Usage: "returns only operations faster than `FAST` milliseconds"},
			{Name: "from", ShortName: "f", Type: StringSourceSlice, Usage: "ignore all entries before `DATE` (see help for date formatting)"},
			{Name: "marker", Type: StringSourceSlice, Usage: "append a pre-defined marker (filename, enum, alpha, none) or custom marker (one per file) identifying the source file of each line"},
			{Name: "message", Type: Bool, Usage: "excludes all non-message portions of each line"},
			{Name: "namespace", Type: String, Usage: "filter by `NAMESPACE` so only lines matching the namespace will be returned"},
			{Name: "pattern", ShortName: "p", Type: String, Usage: "filter queries of shape `PATTERN` (only applies to queries, getmores, updates, removed)"},
			{Name: "severity", ShortName: "i", Type: String, Usage: "find all lines of `SEVERITY`"},
			{Name: "shorten", Type: Int, Usage: "reduces output by truncating log lines to `LENGTH` characters"},
			{Name: "slow", Type: Int, Usage: "returns only operations slower than `SLOW` milliseconds"},
			{Name: "timezone", Type: IntSourceSlice, Usage: "timezone adjustment: add `N` minutes to the corresponding log file"},
			{Name: "to", ShortName: "t", Type: StringSourceSlice, Usage: "ignore all entries after `DATE` (see help for date formatting)"},
			{Name: "word", Type: StringSourceSlice, Usage: "only output lines matching `WORD`"},
		},
	}
	init := func() (Command, error) {
		return &filterCommand{
			Instance: make(map[int]filterInstance),
		}, nil
	}
	GetCommandFactory().Register("filter", args, init)
}

func (f *filterCommand) Finish(index int) error {
	// There are no operations that need to be performed when a file finishes.
	return nil
}

func (f *filterCommand) Prepare(name string, instance int, args CommandArgumentCollection) error {
	opts := filterCommandOptions{
		ConnectionFilter:         -1,
		ExecutionDurationMinimum: -1,
		InvertMatch:              false,
		JsonOutput:               false,
		MarkerOutput:             "",
		MessageOutput:            false,
		ShortenOutput:            0,
		TableScanFilter:          false,
		argCount:                 len(args.Booleans) + len(args.Integers) + len(args.Strings),
	}

	util.Debug("Options: %+v %+v %+v", args.Booleans, args.Integers, args.Strings)
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
	for key, value := range args.Booleans {
		switch key {
		case "exclude":
			opts.InvertMatch = value
		case "message":
			opts.MessageOutput = value
		}
	}

	// parse through all integer arguments
	for key, value := range args.Integers {
		switch key {
		case "connection":
			if value > 0 {
				opts.ConnectionFilter = value
			}
		case "fast":
			if value < 0 {
				return errors.New("--fast must be greater than 0ms")
			}
			opts.FasterFilter = time.Duration(value) * time.Millisecond
		case "shorten":
			if value < 10 {
				return errors.New("--shorten must be longer than 10 characters")
			}
			opts.ShortenOutput = value
		case "slow":
			if value < 1 {
				return errors.New("--slow must be greater than 0ms")
			}
			opts.SlowerFilter = time.Duration(value) * time.Millisecond
		case "timezone":
			if value < -719 || value > 720 {
				return errors.New("--timezone must be an offset between -719 and +720 minutes")
			}
			opts.TimezoneModifier = time.Duration(value) * time.Minute
		}
	}

	// parse through all string arguments
	for key, value := range args.Strings {
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
				opts.MarkerOutput = fmt.Sprintf("%d ", instance)
			} else if value == "alpha" {
				for i := 0; i < instance/26+1; i += 1 {
					opts.MarkerOutput = opts.MarkerOutput + string(rune(instance%26+97))
				}
				opts.MarkerOutput = opts.MarkerOutput + " "
			} else if value == "filename" {
				opts.MarkerOutput = name + " "
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
	f.Instance[instance] = filterInstance{
		Instance:       context.NewInstance(parser.VersionParserFactory.GetAll()),
		commandOptions: opts,
	}
	return nil
}

func (f *filterCommand) Terminate(out chan<- string) error {
	// Finish any
	return nil
}

func (f *filterCommand) Run(instance int, out commandTarget, in commandSource, errs commandError, fatal commandHalt) {
	exit := 0
	options := f.Instance[instance].commandOptions

	go func() {
		<-fatal
		exit = 1
	}()

	// Iterate through every record.Base object provided. This is identical
	// to iterating through every line of a log without multi-line queries.
	for base := range in {
		if exit != 0 {
			// Received an exit signal so immediately exit.
			break
		}

		log := f.Instance[instance]
		entry, err := f.Instance[instance].NewEntry(base)

		if err != nil {
			log.ErrorCount += 1
			if _, ok := err.(parser_errors.VersionUnmatched); ok {
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
			line = base.String()
		}

		if ok := f.match(entry, f.Instance[instance].commandOptions); (options.InvertMatch && ok) || (!options.InvertMatch && !ok) {
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

	// Check the command filter against the command string.
	if opts.CommandFilter != "" && !stringMatchFields(getCmdOrOpFromMessage(entry.Message), opts.CommandFilter) {
		return false
	}

	// Try converting into a base MsgCommand object and do comparisons if the filter succeeds.
	base, ok := record.MsgBaseFromMessage(entry.Message)
	if opts.FasterFilter > 0 && (!ok || time.Duration(base.Duration) > opts.FasterFilter) {
		return false
	} else if opts.SlowerFilter > 0 && (!ok || time.Duration(base.Duration) < opts.SlowerFilter) {
		return false
	} else if opts.NamespaceFilter != "" && (!ok || !stringMatchFields(base.Namespace, opts.NamespaceFilter)) {
		return false
	}

	// Try convergent to a MsgCommandLegacy object and compare filters based on that object type.
	crud, ok := entry.Message.(record.MsgCRUD)
	if !opts.PatternFilter.IsEmpty() && (!ok || !checkQueryPattern(crud.Filter, opts.PatternFilter)) {
		return false
	}

	return true
}

func (f *filterCommand) modify(entry record.Entry, options filterCommandOptions) (record.Entry, bool) {
	if options.TimezoneModifier != 0 && entry.DateValid {
		// add seconds to the parsed date object
		entry.Date = entry.Date.Add(options.TimezoneModifier)
		return entry, true
	}
	return entry, false
}

func checkQueryPattern(query map[string]interface{}, check mongo.Pattern) bool {
	return check.Equals(mongo.NewPattern(query))
}

func getCmdOrOpFromMessage(msg record.Message) string {
	switch t := msg.(type) {
	case record.MsgOperation:
		return t.Operation
	case record.MsgOperationLegacy:
		return t.Operation
	case record.MsgCommand:
		return t.Command
	case record.MsgCommandLegacy:
		return t.Command
	case record.MsgCRUD:
		return getCmdOrOpFromMessage(t.Message)
	default:
		return ""
	}
}

func stringMatchFields(value string, check string) bool {
	for _, item := range util.ArgumentSplit(check) {
		if util.StringInsensitiveMatch(item, value) {
			return true
		}
	}
	return false
}
