package parser

import (
	"mgotools/util"
	"strconv"
	"time"
)

type LogContext struct {
	Count  int
	Errors int

	Hostname string
	Port     int
	Program  string
	Major    int
	Minor    int
	Version  string

	ReplicaSet     bool
	ReplicaState   string
	ReplicaMembers int
	ReplicaVersion int

	DatePreviousMonth time.Month
	DateRollover      int
	DateYearMissing   bool
}

func (c *LogContext) NewLogEntry(raw *RawLogEntry) *LogEntry {
	if raw == nil {
		panic("nil object not allowed when creating a new log entry")
	}

	logEntry := LogEntry{
		RawLogEntry: *raw,

		DateRollover:    0,
		DateValid:       false,
		DateYearMissing: false,
		Valid:           true,
	}

	parseDate := raw.RawDate
	if c.DateYearMissing && util.StringLength(raw.RawDate) > 11 {
		// Take a date in ctime format and add the year.
		parseDate = parseDate[:10] + " " + strconv.Itoa(util.DATE_YEAR+c.DateRollover) + parseDate[10:]
	}

	if date, ok := util.DateParse(parseDate); ok {
		logEntry.Date = date

		if date.Year() == 0 {
			c.DateYearMissing = true
			logEntry.Date = time.Date(time.Now().Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), date.Nanosecond(), date.Location())
		}
	} else {
		logEntry.DateValid = false
	}

	c.Count += 1
	return &logEntry
}
