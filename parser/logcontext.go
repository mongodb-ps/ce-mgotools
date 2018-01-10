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
	factoryFilter   []LogVersionDefinition
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
	Versions []LogVersionDefinition
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
		err error
		out LogEntry = LogEntry{RawLogEntry: raw, Valid: true}
	)
	// Try to parse the date/time of the line.
	remaining := c.factoryCount
	for ; remaining > 0; remaining -= 1 {
		if out.Date, err = c.factories[remaining-1].ParseDate(raw.RawDate); err == nil {
			break
		}
	}
	// No dates matched so mark the date invalid and reset the count.
	if remaining == 0 {
		remaining = c.factoryCount
		out.DateValid = false
	} else {
		out.DateYearMissing = out.Date.Year() == 0
		out.DateValid = true
	}
	out, raw = updateDate(out, raw, c)
	// Try to make some further version guesses based on easily checked information.
	if remaining > 1 {
		switch {
		case raw.CString:
			c.FilterVersions([]LogVersionDefinition{{2, 4, LOG_VERSION_ANY}})
		case raw.RawContext == "[mongosMain]":
			c.FilterVersions([]LogVersionDefinition{{0, 0, LOG_VERSION_MONGOS}})
		case raw.RawComponent == "":
			c.FilterVersions([]LogVersionDefinition{{2, 6, LOG_VERSION_ANY}})
		}
		if c.factoryCount == 1 {
			remaining = 1
		}
	}
	if util.StringLength(out.RawContext) > 2 && IsContext(out.RawContext) {
		out = updateContext(out)
	}
	// Check for the raw message for validity and parse it.
	if out.RawMessage == "" {
		// No log message exists so it cannot be further analyzed.
		out.Valid = false
		if err == nil {
			err = errors.New("no message portion exists")
		}
		c.Errors += 1
		return out, err
	}
	version := false
	// Try parsing the remaining factories for a log message until one succeeds.
	for ; remaining > 0; remaining -= 1 {
		if out.LogMessage, version, err = updateLogMessage(out, c.factories[remaining-1].NewLogMessage); err == nil {
			break
		}
	}
	if version {
		s, _ := out.LogMessage.(LogMsgVersion)
		var logVersion LogVersionDefinition
		if s.Binary == "mongod" {
			logVersion = LogVersionDefinition{s.Major, s.Minor, LOG_VERSION_MONGOD}
		} else if s.Binary == "mongos" {
			logVersion = LogVersionDefinition{s.Major, s.Minor, LOG_VERSION_MONGOS}
		} else {
			logVersion = LogVersionDefinition{s.Major, s.Minor, LOG_VERSION_ANY}
		}
		c.Versions = append(c.Versions, logVersion)
		c.FilterVersions([]LogVersionDefinition{logVersion})
	} else if remaining == 0 {
		// Check for all factory removal and reset
		if c.factoryMaxCount == c.factoryCount {
			// All factories were attempted and all were removed so nothing more to do.
			return out, nil
		}
		// All attempts failed but not all factories were attempted. Reset the factories and try again.
		c.factories, c.factoryCount = makeAllContextFactories()
		if len(c.Versions) > 0 {
			// Occasionally, a version will be directly available from the log file, so we should likely
			// heed that hint. If, for some reason, the log file is a mix of multiple versions then, well,
			// oh well.
			c.FilterVersions(c.Versions)
		} else if c.factoryFilter != nil {
			// Check for a previous version filter. A version filter is generally not called unless there is
			// a specific reason to guess the version accurately.
			c.FilterVersions(c.factoryFilter)
		}
	} else {
		// A match succeeded so reset the factory list to the reduced set.
		c.factories = c.factories[remaining-1:]
		c.factoryCount = len(c.factories)
	}
	updateYearRollover(out, c)
	if !c.preambleParsed && out.LogMessage != nil {
		updatePreamble(out, c)
	}

	c.Count += 1
	c.Lines += 1
	return out, err
}
func updateContext(entry LogEntry) LogEntry {
	entry.Context = entry.RawContext[1 : util.StringLength(entry.RawContext)-1]
	length := util.StringLength(entry.Context)
	if strings.HasPrefix(entry.Context, "conn") && length > 4 {
		entry.Connection, _ = strconv.Atoi(entry.Context[4:])
	} else if strings.HasPrefix(entry.Context, "thread") && length > 6 {
		entry.Thread, _ = strconv.Atoi(entry.Context[6:])
	}
	return entry
}
func (c *LogContext) FilterVersions(a []LogVersionDefinition) {
	y := c.factories[:0]
FactoryCheck:
	for _, f := range c.factories {
		def := f.Version()
		for _, b := range a {
			if (b.Binary == 0 || b.Binary == def.Binary) &&
				(b.Major == (1<<31-1) || b.Major == def.Major) &&
				(b.Minor == (1<<31-1) || b.Minor == def.Minor) {
				y = append(y, f)
				continue FactoryCheck
			}
		}
	}
	if len(y) > 0 {
		c.factories = y
		c.factoryCount = len(y)
		c.factoryFilter = a
	}
}
func makeAllContextFactories() ([]LogVersionParser, int) {
	var dateParserNew = util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL})
	c := []LogVersionParser{
		&LogVersionSCommon{LogVersionCommon{dateParserNew}},
		&LogVersion24Parser{LogVersionCommon{util.NewDateParser([]string{util.DATE_FORMAT_CTIMENOMS, util.DATE_FORMAT_CTIME, util.DATE_FORMAT_CTIMEYEAR})}},
		&LogVersion26Parser{LogVersionCommon{dateParserNew}},
		&LogVersion30Parser{LogVersionCommon{dateParserNew}},
		&LogVersion32Parser{LogVersionCommon{dateParserNew}},
		&LogVersion34Parser{LogVersionCommon{dateParserNew}},
	}
	return c, len(c)
}
func updateDate(entry LogEntry, raw RawLogEntry, c *LogContext) (LogEntry, RawLogEntry) {
	// Handle situations where the date is missing (typically old versions).
	if !c.DateYearMissing && (entry.DateYearMissing || entry.Date.Year() == 0) {
		c.DateYearMissing = true
		entry.Date = time.Date(time.Now().Year(), entry.Date.Month(), entry.Date.Day(), entry.Date.Hour(), entry.Date.Minute(), entry.Date.Second(), entry.Date.Nanosecond(), entry.Date.Location())
	}
	if util.StringLength(raw.RawDate) > 11 {
		// Compensate for dates that do not append a zero to the date.
		if raw.RawDate[9] == ' ' {
			raw.RawDate = raw.RawDate[:8] + "0" + raw.RawDate[8:]
		}
		// Take a date in ctime format and add the year.
		raw.RawDate = raw.RawDate[:10] + " " + strconv.Itoa(util.DATE_YEAR+c.DateRollover) + raw.RawDate[10:]
	}
	return entry, raw
}
func updateLogMessage(entry LogEntry, parser func(LogEntry) (LogMessage, error)) (LogMessage, bool, error) {
	version := false
	if logMessage, err := parser(entry); err != nil {
		return nil, false, err
	} else {
		switch msg := logMessage.(type) {
		case LogMsgConnection:
			entry.Connection = msg.Conn
		case LogMsgVersion:
			version = true
		}
		return logMessage, version, err
	}
}
func updatePreamble(logEntry LogEntry, c *LogContext) {
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
		// Reset the superset of factories since a distinct version should be available.
		c.factoryFilter = nil
		// Apply a single
		switch msg.Binary {
		case "mongod":
			c.Startup[c.startupIndex].DatabaseVersion = msg
			c.FilterVersions([]LogVersionDefinition{{msg.Major, msg.Minor, LOG_VERSION_MONGOD}})
		case "mongos":
			c.Startup[c.startupIndex].ShardVersion = msg
			c.FilterVersions([]LogVersionDefinition{{msg.Major, msg.Minor, LOG_VERSION_MONGOS}})
		case "OpenSSL":
			c.Startup[c.startupIndex].OpenSSLVersion = msg
		}
	case LogMsgListening:
		c.preambleParsed = true
	}
}
func updateYearRollover(logEntry LogEntry, c *LogContext) {
	currentMonth := logEntry.Date.Month()
	if c.DatePreviousMonth != currentMonth {
		if currentMonth == time.January {
			c.DateRollover += 1
		}

		c.DatePreviousMonth = currentMonth
	}
}
