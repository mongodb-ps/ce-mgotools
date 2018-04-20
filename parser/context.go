package parser

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"mgotools/log"
	"mgotools/util"
)

type Context struct {
	in  chan log.Base
	out chan factoryVersionResult

	mutex         sync.Mutex
	rejected      map[LogVersionDefinition]struct{}
	rejectedMutex sync.RWMutex
	startupIndex  int
	wg            sync.WaitGroup

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

	Startup  []contextStartup
	Versions []LogVersionDefinition
}
type factoryVersionResult struct {
	Version  LogVersionDefinition
	Entry    log.Entry
	Err      error
	Rejected bool
}
type contextStartup struct {
	log.MsgBuildInfo
	log.MsgStartupInfo
	log.MsgStartupOptions
	log.MsgWiredTigerConfig

	DatabaseVersion log.MsgVersion
	OpenSSLVersion  log.MsgVersion
	ShardVersion    log.MsgVersion
}

func NewContext() *Context {
	context := Context{
		in:  make(chan log.Base, 8), // These numbers may need tuning at some point.
		out: make(chan factoryVersionResult, 32),

		rejected:     make(map[LogVersionDefinition]struct{}),
		Startup:      []contextStartup{{}},
		startupIndex: 0,

		Count:  0,
		Errors: 0,

		DateRollover:    0,
		DateYearMissing: false,
	}

	// Create a wrapper to determine whether a version has been rejected.
	checkVersion := func(definition LogVersionDefinition) bool {
		context.rejectedMutex.RLock()
		defer context.rejectedMutex.RUnlock()

		_, ok := context.rejected[definition]
		return ok
	}

	// Start the workhorse processor for context checking.
	go run(context.in, context.out, checkVersion, &context.DateRollover)

	return &context
}

func (c *Context) NewEntry(base log.Base) (log.Entry, error) {
	// Send the base entry to be parsed.
	c.in <- base

	// Wait for it to be processed.
	out := <-c.out

	util.Debug("* entry received (line %d)", base.LineNumber)
	if out.Rejected {
		handleRejection(c.rejected, &c.rejectedMutex, func(definition LogVersionDefinition) bool {
			return definition.Compare(out.Version) == 0 && definition.Binary == out.Version.Binary
		})
	}

	if out.Err != nil || out.Rejected {
		util.Debug("* entry rejected, returning (line %d)", base.LineNumber)
		return log.Entry{}, out.Err
	}

	entry := out.Entry
	// Check for compatibility problems with old versions.
	if out.Version.Major == 2 && out.Version.Minor <= 4 {
		// Context (should) guarantee thread-safe behavior because it's possible that whatever methods
		// use it could be concurrent.
		c.mutex.Lock()
		// Date rollover is necessary when the timestamp doesn't include the year. A year is automatically
		// appended to every log.Base entry that doesn't have one. It does this using the current year and
		// a rollover value. Rollover occurs ever time January is detected within the log.
		if currentMonth := entry.Date.Month(); currentMonth < c.DatePreviousMonth {
			// Reset the previous month and year, and update the date rollover.
			c.DateRollover += 1
			c.DatePreviousYear += 1
			currentMonth = time.January
		}
		c.mutex.Unlock()
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
		case log.MsgStartupInfo:
			c.Startup = append(c.Startup, contextStartup{})
			c.startupIndex += 1
			c.Startup[c.startupIndex].MsgStartupInfo = msg
			// Reject all versions since a startup means a new version could be running.
			util.Debug("** version reset from startup info")
			handleRejection(c.rejected, &c.rejectedMutex, func(definition LogVersionDefinition) bool { return true })
		case log.MsgBuildInfo:
			c.Startup[c.startupIndex].MsgBuildInfo = msg
			// Server restarted so reject all versions for a reset (because it could be a new version)
			util.Debug("** Found version reset!")
			handleRejection(c.rejected, &c.rejectedMutex, func(LogVersionDefinition) bool { return true })
		case log.MsgStartupOptions:
			c.Startup[c.startupIndex].MsgStartupOptions = msg
		case log.MsgWiredTigerConfig:
			c.Startup[c.startupIndex].MsgWiredTigerConfig = msg
		case log.MsgVersion:
			// Reject all versions but the current version.
			switch msg.Binary {
			case "mongod":
				c.Startup[c.startupIndex].DatabaseVersion = msg
				handleRejection(c.rejected, &c.rejectedMutex, func(version LogVersionDefinition) bool {
					return version.Major != msg.Major || version.Minor != msg.Minor || version.Binary != LOG_VERSION_MONGOD
				})
			case "mongos":
				c.Startup[c.startupIndex].ShardVersion = msg
				handleRejection(c.rejected, &c.rejectedMutex, func(version LogVersionDefinition) bool {
					return version.Major != msg.Major || version.Minor != msg.Minor || version.Binary != LOG_VERSION_MONGOS
				})
			case "OpenSSL":
				c.Startup[c.startupIndex].OpenSSLVersion = msg
			}
		case log.MsgListening:
			// noop
		}
	}

	util.Debug("* done with new entry (line %d)", base.LineNumber)
	c.Count += 1
	c.Lines += 1
	return entry, nil
}

func quickVersionCheck(base log.Base, check LogVersionDefinition) bool {
	// Order of operations here matter. For example, CString always means version 2.4 but it doesn't
	// say anything about the binary. Therefore, binary checks need to happen before version checks
	// since they never eliminate all results down to a single version and binary pair.
	if base.RawContext == "[mongosMain]" && check.Binary != LOG_VERSION_MONGOS {
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

func handleRejection(rejected map[LogVersionDefinition]struct{}, rejectedMutex sync.Locker, criteria func(LogVersionDefinition) bool) {
	rejectedMutex.Lock()
	defer rejectedMutex.Unlock()

	util.Debug("+ starting rejection (%d rejected)", len(rejected))
	factories := LogVersionParserFactory.Get()
	for _, factory := range factories {
		version := factory.Version()
		if criteria(version) {
			rejected[version] = struct{}{}
		}
	}

	util.Debug("+ ending rejection (%d rejected)", len(rejected))
}

func run(in <-chan log.Base, out chan<- factoryVersionResult, checkVersion func(LogVersionDefinition) bool, rollover *int) {
	util.Debug("starting run method")

	// Alert the calling method of termination.
	defer close(out)

	// Get all registered factories.
	factories := LogVersionParserFactory.Get()
	factoryCountMax := len(factories)
	activeFactoryCount := 0

	util.Debug("# active %d max %d", activeFactoryCount, factoryCountMax)
	// Create a waitgroup to prevent premature exiting.
	wg := sync.WaitGroup{}

	// An array of channels, one for each factory.
	type ParseVersionMux struct {
		Version LogVersionDefinition
		Input   chan log.Base
	}

	parseByVersion := func(baseIn <-chan log.Base, entryOut chan<- factoryVersionResult, parser LogVersionParser, version LogVersionDefinition) {
		wg.Add(1)
		defer wg.Done()

		// Continuously loop over the input channel to process log.Base objects as they arrive.
		result := factoryVersionResult{Version: version}
		for base := range baseIn {
			// Do a quick-and-dirty version check and only process against factories that may return a result.
			result.Rejected = checkVersion(version) || !quickVersionCheck(base, version)
			if !result.Rejected {
				// Run the parser against the active factory (parser).
				r, err := parseLogBase(base, *rollover, parser)

				switch err.(type) {
				case LogVersionDateUnmatched, LogVersionErrorUnmatched:
					result.Rejected = true
				default:
					result.Entry = r
					result.Err = err
				}
			}

			// Create a result object, complete with version, entry result, and errors.
			entryOut <- result
		}
		util.Debug("# ending parseByVersion (%s)", version)
	}

	// Create methods for processing the various factory results.
	reset := func(test chan factoryVersionResult) map[LogVersionDefinition]ParseVersionMux {
		activeFactoryCount = factoryCountMax
		parseVersionCollection := make(map[LogVersionDefinition]ParseVersionMux, factoryCountMax)

		for i := 0; i < factoryCountMax; i += 1 {
			// Each factory gets its own uniquely created channel to receive new entries.
			version := factories[i].Version()
			parseVersionCollection[version] = ParseVersionMux{
				version,
				make(chan log.Base, 4),
			}

			// Create a goroutine per factory to process incoming log.Base objects.
			go parseByVersion(parseVersionCollection[version].Input, test, factories[i], version)
		}

		return parseVersionCollection
	}

	tests := make(chan factoryVersionResult)
	muxIn := reset(tests)

	try := func(entry log.Base) factoryVersionResult {
		// But first make sure there are factories to check against.
		if activeFactoryCount == 0 {
			// Completely reset the number of factories if none are left. The parser should continue running until
			// no more lines exist in the log file.
			muxIn = reset(tests)
		}

		// Loop over each factory and provide a copy of the entry.
		expected := 0
		for _, factoryDefinition := range muxIn {
			factoryDefinition.Input <- entry
			expected += 1
		}

		util.Debug("# sent %d entries for attempts", expected)
		// Create a "winner" object that will be filled with "the winner" out of all the factories attempted.
		var winner *factoryVersionResult = nil
		for i := 0; i < expected; i += 1 {
			// Wait for a result from one of the potential factories and call it an attempt. There is no expectation
			// that results will return in any particular order.
			attempt := <-tests

			util.Debug("# attempt received (line %d): rejected %t", entry.LineNumber, attempt.Rejected)
			// A rejected attempt
			if attempt.Rejected {
				// Check for a matching factory definition and remove it from the array of active factories.
				if _, ok := muxIn[attempt.Version]; ok {
					// Signal the goroutine factory that its lifetime is over. It should exit since reset will create
					// new goroutines of parseByVersion when all factories are done. This prevents unused goroutines
					// from hanging around forever.
					close(muxIn[attempt.Version].Input)
					// Remove the version from the possible factories so future entries won't expect a result.
					delete(muxIn, attempt.Version)
					// Reduce the active factory count.
					activeFactoryCount -= 1
					// Finally, reject the version globally.
					//rejectVersion(attempt.Version)

					util.Debug("# active factory count is now: %d", activeFactoryCount)
				}
				// Ignore rejected attempts since they're clearly not winners.
				continue
			}

			// Pick a winner based on the attempt. The criteria is based on finding the highest potential result
			// by comparing the previous winner (which may be nil) to the new attempt.
			if winner == nil ||
				(winner.Entry.Message == nil && attempt.Entry.Message != nil) ||
				(winner.Version.Compare(attempt.Version) < 0 && attempt.Entry.Message != nil) ||
				winner.Version.Compare(attempt.Version) < 0 && !winner.Entry.Valid && attempt.Entry.Valid {
				// Attempt wins over winner, so replace winner.
				winner = &attempt
			} else if attempt.Err != nil {
				util.Debug("# err version (%d.%d %d)", attempt.Version.Major, attempt.Version.Minor, attempt.Version.Binary)

				switch attempt.Err.(type) {
				case LogVersionMessageUnmatched:
					if winner.Version.Compare(attempt.Version) < 0 {
						winner = &attempt
					}
				case LogVersionDateUnmatched, LogVersionErrorUnmatched:
					if len(factories) == 0 {
						factories = LogVersionParserFactory.Get()
					} else {

					}
				}
			}
		}

		if winner == nil {
			winner = &factoryVersionResult{Err: LogVersionMessageUnmatched{}}
		}

		util.Debug("winner (line %d): (%d.%d %d) %d active", entry.LineNumber, winner.Version.Major, winner.Version.Minor, winner.Version.Binary, activeFactoryCount)
		util.Debug("winner (line %d): %v", entry.LineNumber, *winner)
		return *winner
	}

	// Loop over all incoming log.Base objects and try each factory against a possible winning version.
	for entry := range in {
		util.Debug("# processing new entry with %d factories active", activeFactoryCount)
		// Output the final result.
		out <- try(entry)
	}

	// Close all factory channels to alert the goroutine of completion.
	for _, factory := range muxIn {
		close(factory.Input)
	}

	// Wait for all factory goroutines to complete before returning.
	wg.Wait()
	defer close(tests)
	util.Debug("All factories exhausted! Exiting Context::run.")
}

func parseLogBase(base log.Base, dateModifier int, factory LogVersionParser) (log.Entry, error) {
	var (
		err error
		out = log.Entry{Base: base, DateValid: true, Valid: true}
	)

	if out.Date, err = factory.ParseDate(base.RawDate); err != nil {
		return log.Entry{}, LogVersionDateUnmatched{}
	}

	// No dates matched so mark the date invalid and reset the count.
	out.DateYearMissing = out.Date.Year() == 0
	if util.StringLength(base.RawDate) > 11 {
		// Compensate for dates that do not append a zero to the date.
		if base.RawDate[9] == ' ' {
			base.RawDate = base.RawDate[:8] + "0" + base.RawDate[8:]
		}
		// Take a date in ctime format and add the year.
		base.RawDate = base.RawDate[:10] + " " + strconv.Itoa(util.DATE_YEAR+dateModifier) + base.RawDate[10:]
	}

	if util.StringLength(out.RawContext) > 2 && log.IsContext(out.RawContext) {
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
		return out, LogVersionMessageUnmatched{}
	}

	// Try parsing the remaining factories for a log message until one succeeds.
	out.Message, _ = factory.NewLogMessage(out)
	return out, err
}
