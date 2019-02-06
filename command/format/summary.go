package format

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"mgotools/parser"
	"mgotools/record"
	"mgotools/util"
)

type LogSummary struct {
	Source  string
	Host    string
	Port    int
	Start   time.Time
	End     time.Time
	Length  uint
	Version []parser.VersionDefinition
	Storage string

	mutex sync.Mutex
}

func (s LogSummary) Print(w io.Writer) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	out := bytes.NewBuffer([]byte{})

	host := s.Host
	if host != "" && s.Port > 0 {
		host = fmt.Sprintf("%s:%d", host, s.Port)
	}

	write := func(out *bytes.Buffer, name, value, empty string) {
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

	write(out, "source", s.Source, "")
	write(out, "host", host, "unknown")
	write(out, "start", s.Start.Format("2006 Jan 02 15:04:05.000"), "")
	write(out, "end", s.End.Format("2006 Jan 02 15:04:05.000"), "")
	write(out, "length", strconv.FormatUint(uint64(s.Length), 10), "0")

	var versions = make([]string, len(s.Version))
	for i, v := range s.Version {
		if v.Major < 3 && s.Storage == "" {
			s.Storage = "MMAPv1"
		}

		versions[i] = v.String()
	}

	version := strings.Join(versions, " -> ")
	write(out, "version", version, "unknown")
	write(out, "storage", s.Storage, "unknown")
	out.Write([]byte{'\n'})

	w.Write(out.Bytes())
}

func (s *LogSummary) Update(entry record.Entry) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.Start.IsZero() && entry.DateValid {
		// Only set the start date once (which should coincide with the
		// first line of each instance).
		s.Start = entry.Date
	}

	// Keep the most recent date for log summary purposes.
	s.End = entry.Date

	// Track until the last parsable line number.
	if s.Length < entry.LineNumber {
		s.Length = entry.LineNumber
	}

	switch t := entry.Message.(type) {
	case nil:
		return false

	case record.MsgStartupInfo:
		// The server restarted.
		s.Port = t.Port
		s.Host = t.Hostname

	case record.MsgVersion:
		if t.Major == 2 {
			s.Storage = "MMAPv1"
		}

		var binary record.Binary
		switch t.Binary {
		case "mongod":
			binary = record.BinaryMongod

		case "mongos":
			binary = record.BinaryMongos

		default:
			binary = record.BinaryAny
		}

		s.Version = append(s.Version, parser.VersionDefinition{
			Major:  t.Major,
			Minor:  t.Minor,
			Binary: binary,
		})

		if s.Storage == "" && t.Major < 3 {
			s.Storage = "MMAPv1"
		}

	case record.MsgWiredTigerConfig:
		s.Storage = "WiredTiger"
	}

	return true
}
