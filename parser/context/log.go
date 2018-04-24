package context

import (
	"strconv"
	"strings"
	"time"

	"mgotools/parser"
	"mgotools/record"
	"mgotools/util"
)

type Log struct {
	parserFactory manager
	startupIndex  int

	Count  int
	Errors int
	Lines  int

	DatePreviousMonth time.Month
	DatePreviousYear  int
	DateRollover      int
	DateYearMissing   bool

	ReplicaSet     bool
	ReplicaState   string
	ReplicaMembers int
	ReplicaVersion int

	Startup  []logStartup
	Versions []parser.VersionDefinition
}

type logStartup struct {
	record.MsgBuildInfo
	record.MsgStartupInfo
	record.MsgStartupOptions
	record.MsgWiredTigerConfig

	DatabaseVersion record.MsgVersion
	OpenSSLVersion  record.MsgVersion
	ShardVersion    record.MsgVersion
}

func NewLog() *Log {
	context := Log{
		Startup:      []logStartup{{}},
		startupIndex: 0,

		Count:  0,
		Errors: 0,

		DateRollover:    0,
		DateYearMissing: false,
	}

	context.parserFactory = newManager(context.baseToEntry, parser.VersionParserFactory.Get())
	return &context
}

func (c *Log) NewEntry(base record.Base) (record.Entry, error) {
	manager := c.parserFactory
	entry, version, err := manager.Try(base)

	if err != nil {
		return record.Entry{}, err
	}

	// Check for compatibility problems with old versions.
	if version.Major == 2 && version.Minor <= 4 {
		// Date rollover is necessary when the timestamp doesn't include the year. A year is automatically
		// appended to every log.Base entry that doesn't have one. It does this using the current year and
		// a rollover value. Rollover occurs ever time January is detected within the log.
		if currentMonth := entry.Date.Month(); currentMonth < c.DatePreviousMonth {
			// Reset the previous month and year, and update the date rollover.
			c.DateRollover += 1
			c.DatePreviousYear += 1
		}
	}

	// Handle situations where the date is missing (typically old versions).
	if !c.DateYearMissing && (entry.DateYearMissing || entry.Date.Year() == 0) {
		c.DateYearMissing = true
		entry.Date = time.Date(time.Now().Year(), entry.Date.Month(), entry.Date.Day(), entry.Date.Hour(), entry.Date.Minute(), entry.Date.Second(), entry.Date.Nanosecond(), entry.Date.Location())
	}

	util.Debug("* parsing messages (line %d)", base.LineNumber)
	if entry.Message != nil && entry.Connection == 0 {
		util.Debug("type (line %d): %T", base.LineNumber, entry.Message)
		switch msg := entry.Message.(type) {
		case record.MsgStartupInfo:
			c.Startup = append(c.Startup, logStartup{})
			c.startupIndex += 1
			c.Startup[c.startupIndex].MsgStartupInfo = msg
			// Reset all available versions since the server restarted.
			manager.Reset()
		case record.MsgBuildInfo:
			c.Startup[c.startupIndex].MsgBuildInfo = msg
			// Server restarted so reject all versions for a reset (because it could be a new version)
			manager.Reset()
		case record.MsgStartupOptions:
			c.Startup[c.startupIndex].MsgStartupOptions = msg
		case record.MsgWiredTigerConfig:
			c.Startup[c.startupIndex].MsgWiredTigerConfig = msg
		case record.MsgVersion:
			// Reject all versions but the current version.
			switch msg.Binary {
			case "mongod":
				c.Startup[c.startupIndex].DatabaseVersion = msg
				manager.Reject(func(version parser.VersionDefinition) bool {
					return version.Major != msg.Major || version.Minor != msg.Minor || version.Binary != parser.LOG_VERSION_MONGOD
				})
			case "mongos":
				c.Startup[c.startupIndex].ShardVersion = msg
				manager.Reject(func(version parser.VersionDefinition) bool {
					return version.Major != msg.Major || version.Minor != msg.Minor || version.Binary != parser.LOG_VERSION_MONGOS
				})
			case "OpenSSL":
				c.Startup[c.startupIndex].OpenSSLVersion = msg
			}
		case record.MsgListening:
			// noop
		}
	}

	c.Count += 1
	c.Lines += 1
	return entry, nil
}

func quickVersionCheck(base record.Base, check parser.VersionDefinition) bool {
	// Order of operations here matter. For example, CString always means version 2.4 but it doesn't
	// say anything about the binary. Therefore, binary checks need to happen before version checks
	// since they never eliminate all results down to a single version and binary pair.
	if base.RawContext == "[mongosMain]" && check.Binary != parser.LOG_VERSION_MONGOS {
		util.Debug("mongos")
		// Some version of mongos
		return false
	} else if base.CString {
		// CString is only set for old date formats, which is version 2.4 (or less, which isn't supported)
		return check.Major == 2 && check.Minor == 4
	} else if base.RawComponent == "" {
		// A missing component means version 2.6 of any binary type.
		return check.Major == 2 && check.Minor == 6
	} else {
		return true
	}
}

func (c *Log) baseToEntry(base record.Base, factory parser.VersionParser) (record.Entry, error) {
	var (
		err error
		out = record.Entry{Base: base, DateValid: true, Valid: true}
	)

	if out.Date, err = factory.ParseDate(base.RawDate); err != nil {
		return record.Entry{}, parser.VersionDateUnmatched{}
	}

	// No dates matched so mark the date invalid and reset the count.
	out.DateYearMissing = out.Date.Year() == 0
	if util.StringLength(base.RawDate) > 11 {
		// Compensate for dates that do not append a zero to the date.
		if base.RawDate[9] == ' ' {
			base.RawDate = base.RawDate[:8] + "0" + base.RawDate[8:]
		}
		// Take a date in ctime format and add the year.
		base.RawDate = base.RawDate[:10] + " " + strconv.Itoa(util.DATE_YEAR+c.DateRollover) + base.RawDate[10:]
	}

	if util.StringLength(out.RawContext) > 2 && record.IsContext(out.RawContext) {
		out.Context = out.RawContext[1 : util.StringLength(out.RawContext)-1]
		length := util.StringLength(out.Context)

		if strings.HasPrefix(out.Context, "conn") && length > 4 {
			out.Connection, _ = strconv.Atoi(out.Context[4:])
		} else if strings.HasPrefix(out.Context, "thread") && length > 6 {
			out.Thread, _ = strconv.Atoi(out.Context[6:])
		}
	}

	// Check for the base message for validity and parse it.
	if out.RawMessage == "" {
		// No log message exists so it cannot be further analyzed.
		out.Valid = false
		return out, parser.VersionMessageUnmatched{}
	}

	// Try parsing the remaining factories for a log message until one succeeds.
	out.Message, _ = factory.NewLogMessage(out)
	return out, err
}
