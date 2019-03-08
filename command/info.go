package command

import (
	"bytes"
	"fmt"

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
	context *context.Context
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

func (f *info) Finish(index int, out commandTarget) error {
	instance := f.Instance[index]

	if len(instance.Summary.Version) == 0 {
		versions := f.Instance[index].context.Versions()
		if len(versions) == 1 {
			instance.Summary.Version = versions
		}
	}

	summary := bytes.NewBuffer([]byte{})
	if index > 0 {
		instance.Summary.Divider(summary)
	}

	instance.Summary.Print(summary)

	_, err := instance.output.WriteTo(summary)
	if err != nil {
		return err
	}

	out <- summary.String()
	return nil
}

func (f *info) Prepare(name string, instance int, args ArgumentCollection) error {
	parsers := parser.VersionParserFactory.GetAll()

	f.Instance[instance] = &infoInstance{
		context: context.New(parsers, util.DefaultDateParser.Clone()),
		Summary: format.NewLogSummary(name),
		output:  bytes.NewBuffer([]byte{}),
	}

	if args.Booleans["errors"] {
		f.outputErrors = true
	}

	return nil
}

func (f *info) Run(index int, _ commandTarget, in commandSource, errs commandError) error {
	var exit error

	// Hold a configuration object for future use.
	instance := f.Instance[index]
	summary := &instance.Summary

	// Keep a separate date parser for quick-and-easy entry handling.
	dateParser := util.DefaultDateParser

	// Clean up context resources.
	defer instance.context.Finish()

	iw := newInfoWriter(instance.output)
	alert := func(b record.Entry, m string) {
		iw.WriteString(b.Date.Format(string(b.Format)))
		iw.WriteString(fmt.Sprintf("[line %d]", b.LineNumber))
		iw.WriteString(m)

		if iw.Err() != nil {
			exit = iw.Err()
		}
	}

	for base := range in {
		if exit != nil {
			// Check for an exit signal (in a worryingly un-atomic way).
			return exit
		}

		if base.RawContext != "[initandlisten]" {
			// The only context we care about for updating the summary is
			// "initandlisten" so skipping all other entries will speed things
			// up significantly. The summary still needs to be updated since
			// it maintains a count.
			date, format, err := dateParser.Parse(base.RawDate)

			summary.Update(record.Entry{
				Base:      base,
				Message:   nil,
				Date:      date,
				DateValid: err == nil,
				Format:    format,
			})
			continue
		}

		// Grab an entry from the base record.
		entry, err := instance.context.NewEntry(base)

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
		summary.Guess(instance.context.Versions())
	}

	return nil
}

func (f *info) Terminate(commandTarget) error {
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
