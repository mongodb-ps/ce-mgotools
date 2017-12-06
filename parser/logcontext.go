package parser

import (
	"errors"
	"mgotools/util"
	"strconv"
	"strings"
	"time"
)

type LogContext struct {
	LogEntryFactory

	factories       []LogVersionParser
	factoryCount    int
	factoryMaxCount int
	preambleParsed  bool
	startupIndex    int

	ArgsString map[string]string
	ArgsInt    map[string]int
	ArgsBool   map[string]bool

	Count  int
	Errors int
	Lines  int

	DatePreviousMonth time.Month
	DateRollover      int
	DateYearMissing   bool

	ReplicaSet     bool
	ReplicaState   string
	ReplicaMembers int
	ReplicaVersion int

	Startup  []logContextStartup
	Versions []string
}
type logContextStartup struct {
	LogMsgBuildInfo
	LogMsgStartupInfo
	LogMsgStartupOptions
	LogMsgWiredTigerConfig

	DatabaseVersion LogMsgVersion
	OpenSSLVersion  LogMsgVersion
	ShardVersion    LogMsgVersion
}

func NewLogContext() *LogContext {
	context := &LogContext{
		Startup: []logContextStartup{{}},

		preambleParsed: false,
		startupIndex:   0,

		ArgsBool:   make(map[string]bool),
		ArgsInt:    make(map[string]int),
		ArgsString: make(map[string]string),
		Count:      0,
		Errors:     0,

		DateRollover:    0,
		DateYearMissing: false,
	}
	context.factories, context.factoryCount = makeAllContextFactories()
	context.factoryMaxCount = context.factoryCount
	return context
}
func (c *LogContext) NewRawLogEntry(line string) (RawLogEntry, error) {
	return NewRawLogEntry(line)
}
func (c *LogContext) NewLogEntry(raw RawLogEntry) (LogEntry, error) {
	var (
		err      error
		logEntry LogEntry = LogEntry{RawLogEntry: raw, Valid: true}
	)

	// Try to parse the date/time of the line.
	remaining := c.factoryCount
	for ; remaining > 0; remaining -= 1 {
		if logEntry.Date, err = c.factories[remaining-1].ParseDate(raw.RawDate); err != nil {
			break
		}
	}
	// No dates matched so mark the date invalid and reset the count.
	if remaining == 0 {
		remaining = c.factoryCount
		logEntry.DateValid = false
	} else {
		logEntry.DateYearMissing = logEntry.Date.Year() == 0
		logEntry.DateValid = true
	}
	// Handle situations where the date is missing (typically old versions).
	if !c.DateYearMissing && (logEntry.DateYearMissing || logEntry.Date.Year() == 0) {
		c.DateYearMissing = true
		logEntry.Date = time.Date(time.Now().Year(), logEntry.Date.Month(), logEntry.Date.Day(), logEntry.Date.Hour(), logEntry.Date.Minute(), logEntry.Date.Second(), logEntry.Date.Nanosecond(), logEntry.Date.Location())
	}
	if util.StringLength(raw.RawDate) > 11 {
		// Compensate for dates that do not append a zero to the date.
		if raw.RawDate[9] == ' ' {
			raw.RawDate = raw.RawDate[:8] + "0" + raw.RawDate[8:]
		}
		// Take a date in ctime format and add the year.
		raw.RawDate = raw.RawDate[:10] + " " + strconv.Itoa(util.DATE_YEAR+c.DateRollover) + raw.RawDate[10:]
	}
	// Try to make some further version guesses based on easily checked information.
	if remaining > 1 {
		switch {
		case raw.CString:
			c.factories = []LogVersionParser{&LogVersion24Parser{LogVersionCommon{util.NewDateParser([]string{util.DATE_FORMAT_CTIMENOMS, util.DATE_FORMAT_CTIME, util.DATE_FORMAT_CTIMEYEAR})}}}
		case raw.RawContext == "[mongosMain]":
			c.factories = []LogVersionParser{&LogVersionSCommon{LogVersionCommon{util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL})}}}
		case raw.RawComponent == "":
			c.factories = []LogVersionParser{&LogVersion26Parser{LogVersionCommon{util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL})}}}
		}
	}
	if util.StringLength(logEntry.RawContext) > 2 && util.IsContext(logEntry.RawContext) {
		logEntry.Context = logEntry.RawContext[1: util.StringLength(logEntry.RawContext)-1]
		length := util.StringLength(logEntry.Context)
		if strings.HasPrefix(logEntry.Context, "conn") && length > 4 {
			logEntry.Connection, _ = strconv.Atoi(logEntry.Context[4:])
		} else if strings.HasPrefix(logEntry.Context, "thread") && length > 6 {
			logEntry.Thread, _ = strconv.Atoi(logEntry.Context[6:])
		}
	}
	// Check for the raw message for validity and parse it.
	if logEntry.RawMessage == "" {
		// No log message exists so it cannot be further analyzed.
		logEntry.Valid = false
		if err == nil {
			err = errors.New("no message portion exists")
		}
	} else {
		// Try parsing the remaining factories for a log message until one succeeds.
		for ; remaining > 0; remaining -= 1 {
			if logEntry.LogMessage, err = c.factories[remaining-1].NewLogMessage(logEntry); err == nil {
				util.Debug("%T %+v (%T %+v)", logEntry.LogMessage, logEntry.LogMessage, err, err)
				break
			}
		}
		// Check for all factory removal and reset
		if remaining == 0 {
			if c.factoryMaxCount == c.factoryCount {
				// All factories were attempted and all were removed so nothing more to do.
				logEntry.Valid = false
			} else {
				// All attempts failed but not all factories were attempted. Reset the factories and try again.
				c.factories, c.factoryCount = makeAllContextFactories()
			}
		} else {
			// A match succeeded so reset the factory list to the reduced set.
			c.factories = c.factories[0:remaining-1]
			c.factoryCount = remaining
		}
	}
	switch logEntry.LogMessage.(type) {
	case LogMsgConnection:
		logEntry.Connection = logEntry.LogMessage.(LogMsgConnection).Conn
	}
	if logEntry.Valid {
		c.Count += 1
		currentMonth := logEntry.Date.Month()
		if c.DatePreviousMonth != currentMonth {
			if currentMonth == time.January {
				c.DateRollover += 1
			}

			c.DatePreviousMonth = currentMonth
		}
		if !c.preambleParsed && logEntry.LogMessage != nil {
			switch msg := logEntry.LogMessage.(type) {
			case LogMsgStartupInfo:
				c.Startup = append(c.Startup, logContextStartup{})
				c.startupIndex += 1
				c.Startup[c.startupIndex].LogMsgStartupInfo = msg
			case LogMsgBuildInfo:
				c.Startup[c.startupIndex].LogMsgBuildInfo = msg
			case LogMsgStartupOptions:
				c.Startup[c.startupIndex].LogMsgStartupOptions = msg
			case LogMsgWiredTigerConfig:
				c.Startup[c.startupIndex].LogMsgWiredTigerConfig = msg
			case LogMsgVersion:
				switch msg.Binary {
				case "mongod":
					c.Startup[c.startupIndex].DatabaseVersion = msg
				case "mongos":
					c.Startup[c.startupIndex].ShardVersion = msg
				case "OpenSSL":
					c.Startup[c.startupIndex].OpenSSLVersion = msg
				}
			case LogMsgListening:
				c.preambleParsed = true
			}
		}
	} else {
		c.Errors += 1
	}
	c.Lines += 1
	return logEntry, err
}
func makeAllContextFactories() ([]LogVersionParser, int) {
	var dateParserNew = util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL})
	c := []LogVersionParser{
		&LogVersion24Parser{LogVersionCommon{util.NewDateParser([]string{util.DATE_FORMAT_CTIMENOMS, util.DATE_FORMAT_CTIME, util.DATE_FORMAT_CTIMEYEAR})}},
		&LogVersion26Parser{LogVersionCommon{dateParserNew}},
		&LogVersion30Parser{LogVersionCommon{dateParserNew}},
		&LogVersion32Parser{LogVersionCommon{dateParserNew}},
		&LogVersion34Parser{LogVersionCommon{dateParserNew}},
		&LogVersionSCommon{LogVersionCommon{dateParserNew}},
	}
	return c, len(c)
}
