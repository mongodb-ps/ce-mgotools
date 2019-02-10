package command

import (
	"bytes"
	"fmt"
	"time"

	"mgotools/command/format"
	"mgotools/parser"
	"mgotools/parser/context"
	"mgotools/record"
	"mgotools/util"
)

type info struct {
	outputErrors bool

	Instance map[int]*infoInstance
}

type infoInstance struct {
	*context.Instance

	output  *bytes.Buffer
	Summary format.LogSummary
}

func init() {
	args := Definition{
		Usage: "basic information about a mongodb log",
		Flags: []Argument{
			{Name: "errors", ShortName: "v", Type: Bool, Usage: "output parsing errors to stderr"},
		},
	}

	GetFactory().Register("info", args, func() (Command, error) {
		return &info{
			outputErrors: false,

			Instance: make(map[int]*infoInstance),
		}, nil
	})
}

func (f *info) Finish(index int) error {
	instance := f.Instance[index]
	instance.Finish()

	if index > 0 {
		instance.output.WriteString("\n------------------------------------------\n")
	}

	if len(instance.Summary.Version) == 0 {
		versions := f.Instance[index].Versions()
		if len(versions) == 1 {
			instance.Summary.Version = versions
		}
	}

	summary := bytes.NewBuffer([]byte{})
	instance.Summary.Print(summary)

	_, err := instance.output.WriteTo(summary)
	if err != nil {
		return err
	}

	instance.output = summary
	return nil
}

func (f *info) Prepare(name string, instance int, args ArgumentCollection) error {
	parsers := parser.VersionParserFactory.GetAll()

	f.Instance[instance] = &infoInstance{
		Instance: context.NewInstance(parsers, &util.GlobalDateParser),
		Summary: format.LogSummary{
			Source:  name,
			Host:    "",
			Port:    0,
			Start:   time.Time{},
			End:     time.Time{},
			Length:  0,
			Version: nil,
			Storage: "",
		},
		output: bytes.NewBuffer([]byte{}),
	}

	if args.Booleans["errors"] {
		f.outputErrors = true
	}

	return nil
}

func (f *info) Run(index int, out commandTarget, in commandSource, errs commandError, halt commandHalt) {
	exit := false
	go func() {
		<-halt
		exit = true
	}()

	// Hold a configuration object for future use.
	instance := f.Instance[index]
	summary := &instance.Summary

	iw := newInfoWriter(instance.output)
	alert := func(b record.Entry, m string) {
		iw.WriteString(b.Date.String())
		iw.WriteString(fmt.Sprintf("[line %d]", b.LineNumber))
		iw.WriteString(m)

		if iw.Err() != nil {
			errs <- iw.Err()
			exit = true
		}
	}

	for base := range in {
		if exit {
			// Check for an exit signal (in a worryingly un-atomic way).
			util.Debug("exit signal received")
			return
		}

		// Grab an entry from the base record.
		entry, err := instance.NewEntry(base)

		// Directly output errors in the info module.
		if f.outputErrors && err != nil {
			errs <- err
			continue
		}

		if !summary.Update(entry) {
			continue
		}

		if t, ok := entry.Message.(record.MsgStartupInfo); ok {
			if summary.Host == t.Hostname && summary.Port > 0 && summary.Port != t.Port {
				alert(entry, fmt.Sprintf(
					"The server restarted on a new port (%d -> %d).",
					summary.Port, t.Port))
			} else if summary.Host != "" && summary.Host != t.Hostname && summary.Port == t.Port {
				alert(entry, fmt.Sprintf(
					"The server restarted with a new hostname (%s -> %s).",
					summary.Host, t.Hostname))
			} else if summary.Host != "" && summary.Port > 0 && summary.Host != t.Hostname && summary.Port != t.Port {
				alert(entry, fmt.Sprintf(
					"The server restarted with a new hostname (%s -> %s) and port (%d -> %d).",
					summary.Host, t.Hostname, summary.Port, t.Port))
			}
		}
	}

	if len(summary.Version) == 0 {
		summary.Guess(instance.Versions())
	}
}

func (f *info) Terminate(out chan<- string) error {
	// Iterate through each instance and output the table summary and any
	// messages appended during parsing.
	for _, instance := range f.Instance {
		out <- instance.output.String()
	}

	return nil
}

type infoWriter struct {
	buffer *bytes.Buffer
	err    error
	length int
}

func newInfoWriter(b *bytes.Buffer) *infoWriter {
	return &infoWriter{b, nil, 0}
}

func (i *infoWriter) WriteString(a string) {
	if i.err != nil {
		return
	}

	if i.length > 0 {
		i.err = i.buffer.WriteByte(' ')
		if i.err != nil {
			return
		}
	}

	_, i.err = i.buffer.WriteString(a)
	return
}

func (i *infoWriter) Err() error {
	i.length = 0
	return i.err
}
