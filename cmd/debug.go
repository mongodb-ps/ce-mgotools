package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"mgotools/cmd/factory"
	"mgotools/parser"
	"mgotools/parser/context"
	"mgotools/record"

	"github.com/fatih/color"
)

type debugLog struct {
	json    bool
	context bool
	message bool

	limitLine  bool
	lineNumber []uint

	limitVersion bool
	versions     []parser.VersionDefinition

	width int
}

func init() {
	args := factory.CommandDefinition{
		Usage: "debug log lines",
		Flags: []factory.CommandArgument{
			{Name: "context", ShortName: "c", Type: factory.Bool, Usage: "only check the most likely result"},
			{Name: "json", ShortName: "j", Type: factory.Bool, Usage: "return parsed data in JSON format"},
			{Name: "line", ShortName: "l", Type: factory.String, Usage: "limit by line number"},
			{Name: "message", ShortName: "m", Type: factory.Bool, Usage: "only show messages"},
			{Name: "version", ShortName: "v", Type: factory.String, Usage: "assume parsing of a single version"},
			{Name: "width", ShortName: "w", Type: factory.Int, Usage: "limit line width"},
		},
	}
	init := func() (factory.Command, error) {
		return &debugLog{}, nil
	}

	factory.GetCommandFactory().Register("debug", args, init)
}

func (d *debugLog) Finish(int) error {
	return nil
}

func (d *debugLog) Prepare(c factory.InputContext) error {
	d.context, _ = c.Booleans["context"]
	d.json, _ = c.Booleans["json"]
	d.message, _ = c.Booleans["message"]
	lineArg, _ := c.Strings["line"]
	versionArg, _ := c.Strings["version"]
	width, _ := c.Integers["width"]

	for _, lineString := range strings.Split(lineArg, ",") {
		if lineNum, err := strconv.ParseInt(lineString, 10, 64); err == nil {
			d.lineNumber = append(d.lineNumber, uint(lineNum))
		}
	}
	if len(d.lineNumber) > 0 {
		d.limitLine = true
	}

	parseInt := func(s string) int {
		v, _ := strconv.Atoi(s)
		return v
	}

	parseBinary := func(s string) record.Binary {
		switch s {
		case "d", "mongod", "1":
			return record.BinaryMongod
		case "s", "mongos", "2":
			return record.BinaryMongos
		default:
			return record.BinaryAny
		}
	}

	for _, versionString := range strings.Split(versionArg, ",") {
		parts := strings.Split(versionString, ".")
		if len(parts) == 3 {
			d.versions = append(d.versions, parser.VersionDefinition{
				Major:  parseInt(parts[0]),
				Minor:  parseInt(parts[1]),
				Binary: parseBinary(parts[3]),
			})
		} else if len(parts) == 2 {
			d.versions = append(d.versions, parser.VersionDefinition{
				Major:  parseInt(parts[0]),
				Minor:  parseInt(parts[1]),
				Binary: record.BinaryAny,
			})
		}
	}

	if width > 20 {
		d.width = width
	}
	return nil
}

func (d *debugLog) ProcessLine(index int, out chan<- string, in <-chan string, err chan<- error, halt <-chan struct{}) error {
	exit := false
	go func() {
		<-halt
		exit = true
	}()

	factories := make([]parser.VersionParser, 0)
	for _, check := range parser.VersionParserFactory.Get() {
		if !d.limitVersion || d.checkVersion(check) {
			factories = append(factories, check)
		}
	}

	type BaseResult struct {
		Base record.Base
		Err  error
	}

	type MessageResult struct {
		Msg record.Message
		Err error
	}

	type OutputResult struct {
		Header string
		Body   string
	}

	outputBuffer := make([]OutputResult, 0)
	buffer := func(s, b string) {
		outputBuffer = append(outputBuffer, OutputResult{s, b})
	}

	accumulator := make(chan record.AccumulatorResult)
	go record.Accumulator(in, accumulator, record.NewBase)

	logs := context.NewLog(factories)

	for line := range accumulator {
		base := line.Base
		if d.limitLine && !d.checkLine(base.LineNumber) {
			continue
		}

		messages := make(map[parser.VersionDefinition]MessageResult)

		buffer(fmt.Sprintf("%5d: ", base.LineNumber), base.String())
		buffer("       ", d.formatObject(base))

		if line.Error == nil {
			entry := record.Entry{
				Base:    base,
				Context: strings.Trim(base.RawContext, "[]"),
				Date:    time.Time{},
				Valid:   true,
			}

			if d.context {
				if entry, err := logs.NewEntry(base); err == nil && !d.message {
					buffer("       ", d.formatObject(entry))
				} else if err == nil && d.message && entry.Message != nil {
					buffer("       ", d.formatObject(entry.Message))
				} else {
					buffer(" fail: ", "["+color.RedString(logs.LastWinner.String())+"]")
				}
			} else {
				for _, versionParser := range factories {
					msg, err := versionParser.NewLogMessage(entry)
					messages[versionParser.Version()] = MessageResult{msg, err}
				}

				unmatched := make([]string, 0)
				for v, r := range messages {
					if r.Err == nil {
						buffer(fmt.Sprintf("["+v.String()+"]   "), d.formatObject(r.Msg))
					} else {
						unmatched = append(unmatched, color.RedString(v.String()))
					}
				}
				if len(unmatched) > 0 {
					c := strings.Join(unmatched, ", ")
					buffer(" fail: [", c+"]")
				}
			}
		} else {
			buffer("       ", colorizeObject(line.Error))
		}

		for _, r := range outputBuffer {
			if d.width > 0 && len(r.Body) > d.width {
				r.Body = r.Body[:d.width]
			}

			r.Header = color.HiWhiteString(r.Header)
			out <- r.Header + r.Body
		}

		outputBuffer = outputBuffer[:0]
		out <- ""
	}

	return nil
}

func (d *debugLog) Terminate(chan<- string) error {
	return nil
}

func (d *debugLog) checkLine(current uint) bool {
	for _, lineMatch := range d.lineNumber {
		if current == lineMatch {
			return true
		}
	}
	return false
}

func (d *debugLog) checkVersion(current parser.VersionParser) bool {
	for _, versionMatch := range d.versions {
		if versionMatch.Equals(current.Version()) {
			return true
		}
	}
	return false
}

func (d *debugLog) formatObject(a interface{}) string {
	if !d.json {
		return colorizeObject(a)
	}

	r, err := json.Marshal(a)
	if err != nil {
		return "( ... marshall error ... )"
	}

	return string(r)
}

var errorInterface = (*error)(nil)

func colorizeObject(a interface{}) string {
	b := bytes.Buffer{}

	var m reflect.Value
	if _, ok := a.(reflect.Value); ok {
		m = a.(reflect.Value)
	} else {
		m = reflect.ValueOf(a)
	}

	switch m.Kind() {
	case reflect.Ptr:
		if !m.IsValid() {
			b.WriteString("nil")
		} else {
			b.WriteRune('&')
			b.WriteString(colorizeObject(m.Elem().Interface()))
		}

	case reflect.Struct:
		if m.Type().Implements(reflect.TypeOf(errorInterface).Elem()) {
			b.WriteString(color.HiRedString(m.Type().String()))
		} else {
			b.WriteString(color.BlueString(m.Type().String()))
		}
		b.WriteRune('{')
		for n := 0; n < m.NumField(); n += 1 {
			v := m.Field(n)
			if v.CanInterface() {
				t := m.Type().Field(n)
				if n > 0 {
					b.WriteString(", ")
				}
				if t.Name != v.Type().Name() {
					b.WriteString(color.YellowString(t.Name))
					b.WriteRune(':')
				}
				b.WriteString(colorizeObject(v.Interface()))
			}
		}
		b.WriteRune('}')

	case reflect.Slice:
		b.WriteRune('[')
		for n := 0; n < m.Len(); n += 1 {
			if n > 0 {
				b.WriteString(", ")
			}
			b.WriteString(colorizeObject(m.Index(n).Interface()))
		}
		b.WriteRune(']')

	case reflect.Map:
		for i, key := range m.MapKeys() {
			v := m.MapIndex(key)
			if i == 0 {
				b.WriteString("map[")
				b.WriteString(color.BlueString(key.Type().String()))
				b.WriteRune(']')
				b.WriteString(color.BlueString(v.Type().Name()))
				b.WriteRune('{')
			} else {
				b.WriteString(", ")
			}

			b.WriteString(color.YellowString(key.String()))
			b.WriteRune(':')
			b.WriteString(colorizeObject(v.Interface()))
		}
		b.WriteRune('}')

	case reflect.Interface:
		if m.Type().Implements(reflect.TypeOf(errorInterface).Elem()) {
			b.WriteString(color.HiRedString(m.String()))
		}

	case reflect.Bool:
		b.WriteString(strconv.FormatBool(m.Bool()))

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(m.Int(), 10))

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		b.WriteString(strconv.FormatUint(m.Uint(), 10))

	case reflect.String:
		b.WriteRune('"')
		b.WriteString(m.String())
		b.WriteRune('"')

	case reflect.Float32, reflect.Float64:
		b.WriteString(strconv.FormatFloat(m.Float(), 'f', 2, 64))

	default:
		b.WriteString(m.String())
	}

	return b.String()
}
