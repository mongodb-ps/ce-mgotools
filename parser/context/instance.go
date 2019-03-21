package context

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser"
	"mgotools/record"
	"mgotools/util"
)

type Context struct {
	parserFactory *manager
	versions      []parser.VersionDefinition

	Count      int
	Errors     int
	Lines      int
	LastWinner parser.VersionDefinition

	DatePreviousMonth time.Month
	DatePreviousYear  int
	DateRollover      int
	DateYearMissing   bool

	dateParser *util.DateParser
	day        int
	month      time.Month

	shutdown sync.Once
}

func New(parsers []parser.VersionParser, date *util.DateParser) *Context {
	context := Context{
		Count:  0,
		Errors: 0,

		DateRollover:    0,
		DateYearMissing: false,

		dateParser: date,
		day:        time.Now().Day(),
		month:      time.Now().Month(),
		versions:   make([]parser.VersionDefinition, len(parsers)),
	}

	for index, version := range parsers {
		context.versions[index] = version.Version()
	}

	context.parserFactory = newManager(context.convert, parsers)
	return &context
}

func (c *Context) Versions() []parser.VersionDefinition {
	versions := make([]parser.VersionDefinition, 0)
	for _, check := range c.versions {
		if r, f := c.parserFactory.IsRejected(check); f && !r {
			versions = append(versions, check)
		}
	}

	return versions
}

func (c *Context) Finish() {
	c.shutdown.Do(c.parserFactory.Finish)
}

func (c *Context) NewEntry(base record.Base) (record.Entry, error) {
	manager := c.parserFactory

	// Attempt to retrieve a version from the base.
	entry, version, err := manager.Try(base)
	c.LastWinner = version

	if err == internal.VersionMessageUnmatched {
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
	if entry.DateYearMissing || entry.Date.Year() == 0 {
		c.DateYearMissing = true

		year := time.Now().Year()
		if c.DateRollover == 0 && (entry.Date.Month() > c.month || (entry.Date.Month() == c.month && entry.Date.Day() > c.day)) {
			year -= 1
		}

		entry.Date = time.Date(year, entry.Date.Month(), entry.Date.Day(), entry.Date.Hour(), entry.Date.Minute(), entry.Date.Second(), entry.Date.Nanosecond(), entry.Date.Location())
	}

	// Update index context if it is available.
	if entry.Message != nil && entry.Connection == 0 {
		switch msg := entry.Message.(type) {
		case record.MsgStartupInfo, record.MsgBuildInfo:
			// Reset all available versions since the server restarted.
			manager.Reset()

		case record.MsgVersion:
			// Reject all versions but the current version.
			manager.Reset()

			switch msg.Binary {
			case "mongod":
				manager.Reject(func(version parser.VersionDefinition) bool {
					return version.Major != msg.Major || version.Minor != msg.Minor || version.Binary != record.BinaryMongod
				})
			case "mongos":
				manager.Reject(func(version parser.VersionDefinition) bool {
					return version.Major != msg.Major || version.Minor != msg.Minor || version.Binary != record.BinaryMongos
				})
			}

		case record.MsgListening:
			// noop
		}
	}

	c.Count += 1
	c.Lines += 1
	return entry, nil
}

func (c *Context) convert(base record.Base, factory parser.VersionParser) (record.Entry, error) {
	var (
		err error
		out = record.Entry{Base: base, DateValid: true, Valid: true}
	)

	if out.Date, out.Format, err = c.dateParser.Parse(base.RawDate); err != nil {
		return record.Entry{Valid: false}, internal.VersionDateUnmatched
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

	if util.StringLength(out.RawContext) > 2 && mongo.IsContext(out.RawContext) {
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
		return out, internal.VersionMessageUnmatched
	}

	// Try parsing the remaining factories for a log message until one succeeds.
	out.Message, _ = factory.NewLogMessage(out)
	return out, err
}
