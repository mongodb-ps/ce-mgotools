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

func (f *logFilter) ParseLine(out chan<- string, in <-chan string, signal <-chan int) error {
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
			if ok := f.match(logline); ok {
				out <- logline
			}

			break
		}
	}

	return nil
}

func (f *logFilter) Finish(out chan<- string) error {
	return nil
}

func (f *logFilter) match(line string) bool {
	fmt.Println(fmt.Sprintf("Line: %s", line))
	rawLogEntry := parser.NewRawLogEntry(line)
	logEntry := parser.NewLogEntry(rawLogEntry)

	fmt.Println("Log entry: ", logEntry)
	return false
}
