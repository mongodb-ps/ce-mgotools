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
	Format  map[util.DateFormat]int
	Length  uint
	Version []parser.VersionDefinition
	Storage string

	mutex   sync.Mutex
	guessed bool
}

func NewLogSummary(name string) LogSummary {
	return LogSummary{
		Source:  name,
		Host:    "",
		Port:    0,
		Start:   time.Time{},
		End:     time.Time{},
		Length:  0,
		Version: nil,
		Storage: "",
		Format:  make(map[util.DateFormat]int),
	}
}

func (s *LogSummary) Guess(versions []parser.VersionDefinition) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, v := range versions {
		s.Version = append(s.Version, v)
	}

	s.guessed = true
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

	formatTable := func(histogram map[util.DateFormat]int) string {
		formatString := func(format util.DateFormat) string {
			switch format {
			case util.DATE_FORMAT_CTIME,
				util.DATE_FORMAT_CTIMENOMS:
				return "cdate"
			case util.DATE_FORMAT_CTIMEYEAR:
				return "cdate-year"
			case util.DATE_FORMAT_ISO8602_LOCAL:
				return "iso8602-local"
			case util.DATE_FORMAT_ISO8602_UTC:
				return "iso8602"
			default:
				return "unknown"
			}
		}

		if len(histogram) < 2 {
			for key := range histogram {
				return formatString(key)
			}
			return "unknown"
		} else {
			buffer := bytes.NewBuffer([]byte{})
			total := 0
			for _, count := range histogram {
				total += count
			}
			for format, count := range histogram {
				buffer.WriteString(formatString(format))
				buffer.WriteString(" (")
				buffer.WriteString(strconv.FormatFloat(100*float64(count)/float64(total), 'f', 1, 64))
				buffer.WriteString("%)  ")
			}
			return buffer.String()
		}
	}

	write(out, "source", s.Source, "")
	write(out, "host", host, "unknown")
	write(out, "start", s.Start.Format("2006 Jan 02 15:04:05.000"), "")
	write(out, "end", s.End.Format("2006 Jan 02 15:04:05.000"), "")
	write(out, "date format", formatTable(s.Format), "")
	write(out, "length", strconv.FormatUint(uint64(s.Length), 10), "0")

	var versions = make([]string, 0, len(s.Version))
	for _, v := range s.Version {
		if v.Major < 3 && s.Storage == "" {
			s.Storage = "MMAPv1"
		}

		if v.Major > 1 {
			versions = append(versions, v.String())
		}
	}

	if !s.guessed {
		version := strings.Join(versions, " -> ")
		write(out, "version", version, "unknown")
	} else {
		leastVersion := parser.VersionDefinition{Major: 999, Minor: 999, Binary: record.Binary(999)}

		for _, version := range s.Version {
			if version.Major < leastVersion.Major && leastVersion.Major > 0 {
				leastVersion.Major = version.Major
			}
			if version.Major == leastVersion.Major && version.Minor < leastVersion.Minor {
				leastVersion.Minor = version.Minor
			}
			if version.Major == leastVersion.Major && version.Minor == leastVersion.Minor && version.Binary < leastVersion.Binary && version.Binary > record.BinaryAny {
				leastVersion.Binary = version.Binary
			}
		}

		write(out, "version", fmt.Sprintf("(guess) >= %s", leastVersion.String()), "")
	}

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

	if entry.DateValid {
		s.Format[entry.Format] += 1
	}

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
