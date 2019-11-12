// The debug module provides verbose output about each line of input. The default
// operator will iterate through each parser and return the output. It colorizes
// the output for easy digestion.
//
// +build debug

package command

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"

	"github.com/fatih/color"
)

// Defines a constant that commands may use for debug compiles.
const DEBUG = true

type debugLog struct {
	highlight  string
	json       bool
	context    bool
	message    bool
	object     bool
	patternize bool

	limitLine  bool
	lineNumber []uint

	limitVersion bool
	versions     []version.Definition

	outputBuffer []outputResult

	width int
}

type outputResult struct {
	Header string
	Body   string
}

func init() {
	args := Definition{
		Usage: "debug log lines",
		Flags: []Argument{
			{Name: "context", ShortName: "c", Type: Bool, Usage: "only check the most likely result"},
			{Name: "highlight", ShortName: "g", Type: String, Usage: "highlight specific phrases"},
			{Name: "json", ShortName: "j", Type: Bool, Usage: "return parsed data in JSON format"},
			{Name: "line", ShortName: "l", Type: String, Usage: "limit by line number"},
			{Name: "message", ShortName: "m", Type: Bool, Usage: "only show messages"},
			{Name: "object", Type: Bool, Usage: "only show object details"},
			{Name: "patternize", ShortName: "p", Type: Bool, Usage: "turn queries into a pattern"},
			{Name: "version", ShortName: "v", Type: String, Usage: "assume parsing of a single version"},
			{Name: "width", ShortName: "w", Type: Int, Usage: "limit line width"},
		},
	}
	init := func() (Command, error) {
		return &debugLog{outputBuffer: make([]outputResult, 0)}, nil
	}

	GetFactory().Register("debug", args, init)
}

func (d *debugLog) Finish(int, commandTarget) error {
	return nil
}

func (d *debugLog) Prepare(name string, instance int, args ArgumentCollection) error {
	d.context, _ = args.Booleans["context"]
	d.json, _ = args.Booleans["json"]
	d.highlight, _ = args.Strings["highlight"]
	d.message, _ = args.Booleans["message"]
	d.patternize, _ = args.Booleans["patternize"]
	d.object, _ = args.Booleans["object"]
	lineArg, _ := args.Strings["line"]
	versionArg, _ := args.Strings["version"]
	width, _ := args.Integers["width"]

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
			d.versions = append(d.versions, version.Definition{
				Major:  parseInt(parts[0]),
				Minor:  parseInt(parts[1]),
				Binary: parseBinary(parts[2]),
			})
		} else if len(parts) == 2 {
			d.versions = append(d.versions, version.Definition{
				Major:  parseInt(parts[0]),
				Minor:  parseInt(parts[1]),
				Binary: record.BinaryAny,
			})
		}
	}
	if len(d.versions) > 0 {
		d.limitVersion = true
	}

	if width > 20 {
		d.width = width
	}
	d.highlight = strings.ToLower(d.highlight)
	return nil
}

func (d *debugLog) Run(instance int, out commandTarget, in commandSource, errs commandError) error {
	type BaseResult struct {
		Base record.Base
		Err  error
	}

	type MessageResult struct {
		Msg message.Message
		Err error
	}

	// Instantiate a list of factories. All factories will be run by default
	// unless the --version switch is provided.
	factories := make([]version.Parser, 0)
	for _, check := range version.Factory.GetAll() {
		if !d.limitVersion || d.checkVersion(check) {
			factories = append(factories, check)
		}
	}

	// Create a wait group to ensure everything exits gracefully.
	var waitGroup sync.WaitGroup
	waitGroup.Add(1)

	buffer := d.buffer
	logs := version.New(factories, internal.DefaultDateParser.Clone())

	versionLogs := make(map[version.Definition]*version.Context)
	for _, f := range factories {
		waitGroup.Add(1)
		versionLogs[f.Version()] = version.New([]version.Parser{f}, internal.DefaultDateParser.Clone())
	}

	for base := range in {
		if d.limitLine && !d.checkLine(base.LineNumber) {
			continue
		}

		messages := make(map[version.Definition]MessageResult)

		if !d.object {
			buffer(fmt.Sprintf("%5d: ", base.LineNumber), base.String())
			//buffer("       ", d.formatObject(base))
		}

		if d.context {
			if entry, err := logs.NewEntry(base); err == nil && !d.message {
				buffer("       ", d.formatObject(entry))
			} else if err == nil && d.message && entry.Message != nil {
				buffer("       ", d.formatObject(entry.Message))
			} else if !d.object {
				buffer(" fail: ", fmt.Sprintf("[%s] (err: %v)", color.RedString(logs.LastWinner.String()), err))
			}
		} else {
			for _, versionParser := range factories {
				if pass := versionParser.Check(base); !pass && !d.object {
					buffer(" skip: ", fmt.Sprintf("[%s]", color.HiCyanString(versionParser.Version().String())))
				} else if entry, err := versionLogs[versionParser.Version()].Convert(base, versionParser); err != nil && !d.object {
					buffer(" fail: ", fmt.Sprintf("[%s] (err: %v)", color.RedString(versionParser.Version().String()), err))
				} else if d.object && entry.Message != nil || !d.object {
					messages[versionParser.Version()] = MessageResult{entry.Message, err}
				}
			}

			unmatched := make([]string, 0)
			for v, r := range messages {
				if r.Err == nil {
					prefix := fmt.Sprintf("[" + v.String() + "]   ")
					buffer(prefix, d.formatObject(r.Msg))

					if d.patternize {
						if p, ok := message.PayloadFromMessage(r.Msg); ok && p != nil {
							pattern := mongo.NewPattern(*p)
							buffer(prefix+color.WhiteString("--> "), pattern.String())
						}
					}
				} else if !d.object {
					unmatched = append(unmatched, color.RedString(v.String()))
				}
			}
			if len(unmatched) > 0 && !d.object {
				c := strings.Join(unmatched, ", ")
				buffer(" fail: [", c+"]")
			}
		}

		d.flush(out)
	}

	// Finalize the logs instance.
	go func() {
		defer waitGroup.Done()
		logs.Finish()
	}()

	// Finalize each version instance.
	go func() {
		for _, version := range versionLogs {
			version.Finish()
			waitGroup.Done()
		}
	}()

	waitGroup.Wait()

	return nil
}

func (d *debugLog) Terminate(commandTarget) error {
	return nil
}

func (d *debugLog) buffer(s, b string) {
	if d.highlight != "" {
		var colorize func(string, string, func(string, ...interface{}) string) string
		colorize = func(m string, h string, c func(string, ...interface{}) string) string {
			if l := len(h); len(m) > 0 && l > 0 {
				if pos := strings.Index(strings.ToLower(m), h); pos > -1 {
					r := []byte(m)
					r = append(r[:pos], r[pos+l:]...)
					r = append(r[:pos], append([]byte(c(m[pos:pos+l])), []byte(colorize(string(r[pos:]), h, c))...)...)
					m = string(r)
				}
			}
			return m
		}
		s := strings.Split(d.highlight, "||")
		c := []func(string, ...interface{}) string{
			color.RedString,
			color.GreenString,
			color.CyanString,
			color.HiMagentaString,
		}
		for i, v := range s {
			b = colorize(b, v, c[i%len(c)])
		}
	}

	d.outputBuffer = append(d.outputBuffer, outputResult{s, b})
}

func (d *debugLog) checkLine(current uint) bool {
	for _, lineMatch := range d.lineNumber {
		if current == lineMatch {
			return true
		}
	}
	return false
}

func (d *debugLog) checkVersion(current version.Parser) bool {
	for _, versionMatch := range d.versions {
		if versionMatch.Binary == record.BinaryAny && versionMatch.Compare(current.Version()) == 0 {
			return true
		} else if versionMatch.Equals(current.Version()) {
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

func (d *debugLog) flush(out commandTarget) {
	for _, r := range d.outputBuffer {
		if d.width > 0 && len(r.Body) > d.width {
			r.Body = r.Body[:d.width]
		}

		r.Header = color.HiWhiteString(r.Header)
		out <- r.Header + r.Body
	}

	d.outputBuffer = d.outputBuffer[:0]

	if !d.object {
		out <- ""
	}
}

var errorInterface = (*error)(nil)

func colorizeObject(a interface{}) string {
	b := bytes.Buffer{}
	if a == nil {
		return "nil"
	}

	var m reflect.Value
	if _, ok := a.(reflect.Value); ok {
		m = a.(reflect.Value)
	} else {
		m = reflect.ValueOf(a)
	}

	switch m.Kind() {
	case reflect.Ptr:
		if !m.IsValid() || m.IsNil() {
			b.WriteString(color.HiWhiteString("nil"))
		} else {
			b.WriteString(color.HiWhiteString("&"))
			b.WriteString(colorizeObject(m.Elem().Interface()))
		}

	case reflect.Struct:
		if m.Type().Implements(reflect.TypeOf(errorInterface).Elem()) {
			b.WriteString(color.HiRedString(m.Type().String()))
		} else {
			b.WriteString(color.BlueString(m.Type().String()))
		}
		b.WriteString(color.HiWhiteString("{"))
		count := 0
		for n := 0; n < m.NumField(); n += 1 {
			v := m.Field(n)
			if v.CanInterface() {
				count += 1
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
		if count == 0 && m.NumMethod() > 0 {
			if r := m.MethodByName("String").Call([]reflect.Value{}); len(r) > 0 {
				b.WriteString(r[0].String())
			}
		}
		b.WriteString(color.HiWhiteString("}"))

	case reflect.Slice:
		b.WriteString(color.HiWhiteString("["))
		for n := 0; n < m.Len(); n += 1 {
			if n > 0 {
				b.WriteString(", ")
			}
			b.WriteString(colorizeObject(m.Index(n).Interface()))
		}
		b.WriteString(color.HiWhiteString("]"))

	case reflect.Map:
		if m.IsNil() {
			b.WriteString(color.HiWhiteString("nil"))
			break
		}

		t := m.Type()
		b.WriteString(color.HiBlueString("map["))
		b.WriteString(color.BlueString(t.Key().String()))
		b.WriteString(color.HiBlueString("]"))
		if t.Elem().Kind() != reflect.Interface {
			b.WriteString(color.BlueString(t.Elem().String()))
		}
		b.WriteString(color.HiBlueString("{"))

		for i, key := range m.MapKeys() {
			v := m.MapIndex(key)
			if i > 0 {
				b.WriteString(", ")
			}

			b.WriteString(color.YellowString(key.String()))
			b.WriteRune(':')
			b.WriteString(colorizeObject(v.Interface()))
		}
		b.WriteString(color.HiBlueString("}"))

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
		b.WriteString(strconv.FormatFloat(m.Float(), 'f', 1, 64))

	default:
		b.WriteString(m.String())
	}

	return b.String()
}
