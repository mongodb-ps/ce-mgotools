package parser

import (
	"mgotools/util"
	"strconv"
	"time"
	"strings"
)

type LogContextParser interface {
	NewLogEntry(*RawLogEntry, bool) *LogEntry
}

type LogContext struct {
	preambleParsed bool

	Count           int
	Errors          int
	LogEntryOptions LogEntryOptions

	BuildInfo  string
	DbPath     string
	Hostname   string
	Major      int
	Minor      int
	OpenSSL    string
	Options    string
	Pid        int
	Port       int
	Program    string
	Version    string
	WiredTiger string

	DatePreviousMonth time.Month
	DateRollover      int
	DateYearMissing   bool

	ReplicaSet     bool
	ReplicaState   string
	ReplicaMembers int
	ReplicaVersion int
}

func NewLogContext() *LogContext {
	return &LogContext{
		preambleParsed: false,

		Count:  0,
		Errors: 0,
		LogEntryOptions: LogEntryOptions{
			ParseDate: true,
		},

		DateRollover:    0,
		DateYearMissing: false,
	}
}

func (c *LogContext) NewLogEntry(raw *RawLogEntry, updateContext bool) *LogEntry {
	if raw == nil {
		panic("nil object not allowed when creating a new log entry")
	}

	var (
		logEntry *LogEntry
	)

	if !c.DateYearMissing {
		logEntry = NewLogEntry(raw, &c.LogEntryOptions)

		if logEntry.DateYearMissing || logEntry.Date.Year() == 0 {
			if updateContext {
				c.DateYearMissing = true
			}

			logEntry.Date = time.Date(time.Now().Year(), logEntry.Date.Month(), logEntry.Date.Day(), logEntry.Date.Hour(), logEntry.Date.Minute(), logEntry.Date.Second(), logEntry.Date.Nanosecond(), logEntry.Date.Location())
		}
	} else if util.StringLength(raw.RawDate) > 11 {
		// Take a date in ctime format and add the year.
		raw.RawDate = raw.RawDate[:10] + " " + strconv.Itoa(util.DATE_YEAR+c.DateRollover) + raw.RawDate[10:]
		logEntry = NewLogEntry(raw, &c.LogEntryOptions)
	}

	if logEntry.Valid {
		c.Count += 1

		if updateContext {
			currentMonth := logEntry.Date.Month()
			if c.DatePreviousMonth != currentMonth {
				if currentMonth == time.January {
					c.DateRollover += 1
				}

				c.DatePreviousMonth = currentMonth
			}

			if !c.preambleParsed {
				c.UpdateContext(logEntry)
			}
		}
	} else {
		c.Errors += 1
	}

	return logEntry
}

func (c *LogContext) UpdateContext(logEntry *LogEntry) {
	switch logEntry.Context {
	case "initandlisten":
		if c.BuildInfo == "" && strings.HasPrefix(logEntry.RawMessage, "build info: ") {
			c.BuildInfo = logEntry.RawMessage[12:]
		} else if c.Version == "" && strings.HasPrefix(logEntry.RawMessage, "db version v") && util.StringLength(logEntry.RawMessage) > 12 {
			if c.Version = logEntry.RawMessage[12:]; c.Version != "" {
				if version := strings.Split(c.Version, "."); len(version) >= 2 {
					c.Major, _ = strconv.Atoi(version[0])
					c.Minor, _ = strconv.Atoi(version[1])
				}
			}
		} else if c.Pid == 0 && strings.HasPrefix(logEntry.RawMessage, "MongoDB starting") {
			if optionsRegex, err := util.GetRegexRegistry().Compile(`([^=\s]+)=([^\s]+)`); err == nil {
				matches := optionsRegex.FindAllStringSubmatch(logEntry.RawMessage, -1)

				for _, match := range matches {
					switch match[1] {
					case "dbpath":
						c.DbPath = match[2]
						break

					case "host":
						c.Hostname = match[2]
						break

					case "pid":
						c.Pid, _ = strconv.Atoi(match[2])
						break

					case "port":
						c.Port, _ = strconv.Atoi(match[2])
						break
					}
				}
			}
		} else if c.OpenSSL == "" && strings.HasPrefix(logEntry.RawMessage, "OpenSSL version: ") && util.StringLength(logEntry.RawMessage) > 17 {
			c.OpenSSL = logEntry.RawMessage[17:]
		} else if c.Options == "" && strings.HasPrefix(logEntry.RawMessage, "options: ") && util.StringLength(logEntry.RawMessage) > 9 {
			c.Options = logEntry.RawMessage[9:]
		} else if c.WiredTiger == "" && strings.HasPrefix(logEntry.RawMessage, "wiredtiger_open config: ") {
			c.WiredTiger = logEntry.RawMessage[24:]
		} else if !c.preambleParsed && strings.HasPrefix(logEntry.RawMessage, "waiting for connections") {
			c.preambleParsed = false
		}

		break

	case "signalProcessingThread":
		// Shutdown message.
		break
	}
}
