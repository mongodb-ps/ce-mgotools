package format

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"mgotools/parser"
	"mgotools/record"
	"mgotools/util"
)

type LogSummary struct {
	Source     string
	Host       string
	Port       int
	Start      time.Time
	End        time.Time
	DateFormat string
	Length     int64
	Version    []parser.VersionDefinition
	Storage    string
}

func PrintLogSummary(s LogSummary, w io.Writer) {
	out := bytes.NewBuffer([]byte{})
	var binary record.Binary

	host := s.Host
	if host != "" && s.Port > 0 {
		host = fmt.Sprintf("%s:%d", host, s.Port)
	}

	writeLine(out, "source", s.Source, "")
	writeLine(out, "host", host, "unknown")
	writeLine(out, "start", s.Start.Format("2006 Jan 02 15:04:05.000"), "")
	writeLine(out, "end", s.End.Format("2006 Jan 02 15:04:05.000"), "")
	writeLine(out, "date format", s.DateFormat, "unknown")
	writeLine(out, "length", strconv.FormatInt(int64(s.Length), 10), "0")

	versionsToString := func() []string {
		r := make([]string, len(s.Version))
		for i, v := range s.Version {
			r[i] = v.String()
			binary = v.Binary
		}
		return r
	}

	version := strings.Join(versionsToString(), " -> ")
	writeLine(out, "binary", binary.String(), "unknown")
	writeLine(out, "version", version, "unknown")
	writeLine(out, "storage", version, s.Storage)
	out.Write([]byte{'\n'})

	w.Write(out.Bytes())
}

func writeLine(out *bytes.Buffer, name, value, empty string) {
	if value == "" && empty == "" {
		return
	}
	if util.StringLength(name) < 11 {
		out.WriteString(strings.Repeat(" ", 11-util.StringLength(name)))
	}
	out.WriteString(name)
	out.WriteString(": ")

	if value != "" {
		out.WriteString(value)
	} else {
		out.WriteString(empty)
	}
	out.WriteRune('\n')
}
