package commands

import (
	"fmt"
	"mgotools/parser"
	"time"
)

type logFilter struct {
	BaseOptions

	CommandFilter            string
	ExecutionDurationMinimum int
	FromFilter               time.Time
	InvertMatch              bool
	JsonOutput               bool
	NamespaceFilter          string
	OperationFilter          string
	PatternFilter            string
	Shorten                  bool
	TableScanFilter          bool
	TimestampModifier        int
	ToFilter                 time.Time
}

func (f *logFilter) ParseLine(out chan<- string, in <-chan string, signal <-chan int, context *parser.LogContext) error {
	var exit int = 0
	go func() {
		exit = <-signal
	}()

	for logline := range in {
		switch {
		case exit != 0:
			// Received an exit signal so immediately exit.
			fmt.Println("Received exit signal")
			return nil

		default:
			if ok := f.match(logline, context); ok {
				out <- logline
			}

			break
		}
	}

	fmt.Println()
	fmt.Println(fmt.Sprintf("%+v", context))
	return nil
}

func (f *logFilter) Finish(out chan<- string) error {
	return nil
}

func (f *logFilter) match(line string, context *parser.LogContext) bool {
	logEntry := context.NewLogEntry(parser.NewRawLogEntry(line), true)

	if !logEntry.Valid {
		return false
	}

	return true
}
