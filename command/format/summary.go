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

func (LogSummary) Divider(w io.Writer) {
	w.Write([]byte("\n------------------------------------------\n"))
}

func (s LogSummary) Print(w io.Writer) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	host := s.Host
	if host != "" && s.Port > 0 {
		host = fmt.Sprintf("%s:%d", host, s.Port)
	}

	write := func(out io.Writer, name, value, empty string) {
		if value == "" && empty == "" {
			return
		}
		if util.StringLength(name) < 11 {
			out.Write([]byte(strings.Repeat(" ", 11-util.StringLength(name))))
		}
		out.Write([]byte(name))
		out.Write([]byte(": "))

		if value != "" {
			out.Write([]byte(value))
		} else {
			out.Write([]byte(empty))
		}

		out.Write([]byte("\n"))
	}

	formatTable := func(histogram map[util.DateFormat]int) string {
		formatString := func(format util.DateFormat) string {
			switch format {
			case util.DateFormatCtime,
				util.DateFormatCtimenoms:
				return "cdate"
			case util.DateFormatCtimeyear:
				return "cdate-year"
			case util.DateFormatIso8602Local:
				return "iso8602-local"
			case util.DateFormatIso8602Utc:
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

	write(w, "source", s.Source, "")
	write(w, "host", host, "unknown")
	write(w, "start", s.Start.Format("2006 Jan 02 15:04:05.000"), "")
	write(w, "end", s.End.Format("2006 Jan 02 15:04:05.000"), "")
	write(w, "date format", formatTable(s.Format), "")
	write(w, "length", strconv.FormatUint(uint64(s.Length), 10), "0")

	var versions = make([]string, 0, len(s.Version))
	for _, v := range s.Version {
		if v.Major < 3 && s.Storage == "" {
			s.Storage = "MMAPv1"
		}

		if v.Major > 1 && (len(versions) == 0 || versions[len(versions)-1] != v.String()) {
			versions = append(versions, v.String())
		}
	}

	if !s.guessed {
		version := strings.Join(versions, " -> ")
		write(w, "version", version, "unknown")
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

		write(w, "version", fmt.Sprintf("(guess) >= %s", leastVersion.String()), "")
	}

	write(w, "storage", s.Storage, "unknown")
	w.Write([]byte{'\n'})
}

func (s *LogSummary) Update(entry record.Entry) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.Start.IsZero() && entry.DateValid {
		// Only set the start date once (which should coincide with the
		// first line of each instance).
		s.Start = entry.Date
	}

	if !entry.Date.IsZero() && entry.DateValid {
		// Keep the most recent date for log summary purposes.
		s.End = entry.Date
	}

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
		s.options(t.Port, t.Hostname)

	case record.MsgStartupInfoLegacy:
		s.options(t.Port, t.Hostname)
		s.version(t.MsgVersion)

	case record.MsgVersion:
		s.version(t)

	case record.MsgWiredTigerConfig:
		s.Storage = "WiredTiger"
	}

	return true
}

func (s *LogSummary) options(port int, hostname string) {
	// The server restarted.
	s.Port = port
	s.Host = hostname
}

func (s *LogSummary) version(msg record.MsgVersion) {
	if msg.Major == 2 {
		s.Storage = "MMAPv1"
	} else if (msg.Major == 4 && msg.Minor > 0) || msg.Major > 4 {
		s.Storage = "WiredTiger"
	}
	var binary record.Binary
	switch msg.Binary {
	case "mongod":
		binary = record.BinaryMongod

	case "mongos":
		binary = record.BinaryMongos

	default:
		binary = record.BinaryAny
	}
	s.Version = append(s.Version, parser.VersionDefinition{
		Major:  msg.Major,
		Minor:  msg.Minor,
		Binary: binary,
	})
	if s.Storage == "" && msg.Major < 3 {
		s.Storage = "MMAPv1"
	}
}
