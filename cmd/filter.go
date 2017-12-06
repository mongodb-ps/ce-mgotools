package cmd

import (
	"fmt"
	"github.com/pkg/errors"
	"mgotools/parser"
	"mgotools/util"
	"strings"
	"time"
)

type filterCommandOptions struct {
	CommandFilter            string
	ExecutionDurationMinimum int
	FromFilter               time.Time
	InvertMatch              bool
	JsonOutput               bool
	MessageOutput            bool
	NamespaceFilter          string
	OperationFilter          string
	PatternFilter            string
	ShortenOutput            int
	TableScanFilter          bool
	TimezoneModifier         time.Duration
	ToFilter                 time.Time

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

type filterCommand struct {
	BaseOptions
	Log []filterLog
}

func (f *filterCommand) Finish(out chan<- string) error {
	//out <- fmt.Sprintf("final counts: %d lines, %d errors", f.lineCount, f.errorCount)
	return nil
}

func (f *filterCommand) ProcessLine(index int, out chan<- string, in <-chan string, errs chan<- error, fatal <-chan struct{}) error {
	var (
		options filterCommandOptions
		exit    int = 0
	)
	go func() {
		<-fatal
		exit = 1
	}()
	options = f.Log[index].commandOptions
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
			}
			if entry, modified := f.modify(entry, options); modified {
				line = entry.String()
			}

			if ok := f.match(entry, index, options); (!options.InvertMatch && ok) || (options.InvertMatch && !ok) {
				if options.ShortenOutput > 0 {
					line = entry.Prefix(options.ShortenOutput)
				} else if options.MessageOutput {
					line = entry.RawMessage
				}
				//out <- line
				out <- fmt.Sprintf("%s\n\t%+v", line, entry.LogMessage)
			}
		}
	}
	return nil
}

func (f *filterCommand) Init() {
}

func (f *filterCommand) Prepare(index int, factory parser.LogEntryFactory, booleans map[string]bool, integers map[string]int, strings map[string]string) error {
	options := filterCommandOptions{}
	options.argCount = len(booleans) + len(integers) + len(strings)

	// parse through all boolean arguments
	for key, value := range booleans {
		switch key {
		case "exclude":
			options.InvertMatch = value
		case "message":
			options.MessageOutput = value
		}
	}

	// parse through all integer arguments
	for key, value := range integers {
		switch key {
		case "shorten":
			if value < 10 {
				return errors.New("--shorten must be longer than 10 characters")
			}
			options.ShortenOutput = value
		case "timezone":
			if value < -719 || value > 720 {
				return errors.New("--timezone must be an offset between -719 and +720 minutes")
			}
			options.TimezoneModifier = time.Duration(value)
		}
	}

	f.Log[index] = filterLog{
		LogEntryFactory: factory,
		argsBool:        booleans,
		argsInt:         integers,
		argsString:      strings,
		commandOptions:  options,
	}

	// parse through all string arguments
	for key, value := range strings {
		if value == "" {
			return errors.New(fmt.Sprintf("%s cannot be empty", key))
		}

		dateParser := util.NewDateParser([]string{
			"2006",
			"2016-01-02",
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

		switch {
		case key == "component" && !util.ArgumentMatchOptions(util.COMPONENTS, value):
			return errors.New("--component is not a recognized component")
		case key == "severity" && !util.ArgumentMatchOptions(util.SEVERITIES, value):
			return errors.New("--severity is not a recognized severity")
		case key == "from":
			if dateParser, err := dateParser.ParseDate(value); err != nil {
				options.FromFilter = dateParser
				fmt.Println(fmt.Sprintf("From: %s", dateParser.String()))
				break
			}
			return errors.New("--from flag could not be parsed")
		case key == "to":
			if dateParser, err := dateParser.ParseDate(value); err != nil {
				options.ToFilter = dateParser
				fmt.Println(fmt.Sprintf("To  : %s", dateParser.String()))
				break
			}
			return errors.New("--to flag could not be parsed")
		}
	}

	return nil
}

func (f *filterCommand) Usage() string {
	return "used to filter log files based on a set of criteria"
}

func (f *filterCommand) match(logEntry parser.LogEntry, index int, options filterCommandOptions) bool {
	if !logEntry.Valid {
		return false
	}

	if options.argCount == 0 {
		return true
	}

	return checkBoolArguments(logEntry, f.Log[index].argsBool, options) &&
		checkIntArguments(logEntry, f.Log[index].argsInt, options) &&
		checkStringArguments(logEntry, f.Log[index].argsString, options)
}

func (f *filterCommand) modify(entry parser.LogEntry, options filterCommandOptions) (parser.LogEntry, bool) {
	modified := false
	if options.TimezoneModifier != 0 && entry.DateValid {
		// add seconds to the parsed date object
		entry.Date = entry.Date.Add(options.TimezoneModifier * time.Minute)
		modified = true
	}
	return entry, modified
}

func checkBoolArguments(logEntry parser.LogEntry, args map[string]bool, options filterCommandOptions) bool {
	/*for key, value := range args {
		switch {
		default:
		}
	}*/
	return true
}

func checkIntArguments(logEntry parser.LogEntry, args map[string]int, options filterCommandOptions) bool {
	for key, value := range args {
		switch {
		case key == "connection":
			if logEntry.Connection != value {
				return false
			}
		}
	}
	return true
}

func checkStringArguments(logEntry parser.LogEntry, args map[string]string, options filterCommandOptions) bool {
	var checkString string
	for key, value := range args {
		switch {
		case key == "component":
			checkString = logEntry.RawComponent
		case key == "context":
			checkString = logEntry.Context
		case key == "severity":
			checkString = logEntry.RawSeverity
		case key == "from" && (!logEntry.DateValid || options.FromFilter.After(logEntry.Date)):
			return false
		case key == "to" && (!logEntry.DateValid || options.ToFilter.Before(logEntry.Date)):
			return false
		}
		if !stringMatchFields(value, checkString) {
			return false
		}
	}
	return true
}

func stringMatchFields(value string, check string) bool {
	for _, item := range strings.FieldsFunc(value, util.ArgumentSplit) {
		if util.StringInsensitiveMatch(item, check) {
			return true
		}
	}
	return false
}
