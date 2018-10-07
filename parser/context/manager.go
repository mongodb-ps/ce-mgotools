package context

import (
	"sync"

	"mgotools/parser"
	"mgotools/record"
	"mgotools/util"
)

type Result struct {
	Entry    record.Entry
	Err      error
	Rejected bool
	Version  parser.VersionDefinition
}

type logParser struct {
	Errors   uint
	Input    chan record.Base
	Parser   parser.VersionParser
	Rejected bool
	Tries    uint
	Version  parser.VersionDefinition
	Wins     uint
}

type manager struct {
	mutex     sync.RWMutex
	output    chan Result
	rejected  uint32
	versions  map[parser.VersionDefinition]*logParser
	waitGroup sync.WaitGroup
}

func (m *manager) IsRejected(c parser.VersionDefinition) (bool, bool) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for t, r := range m.versions {
		if c.Equals(t) {
			return r.Rejected, true
		}
	}

	return false, false
}

func (m *manager) Reject(test func(parser.VersionDefinition) bool) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	count := uint32(0)
	for version := range m.versions {
		if test(version) {
			count += 1
			m.versions[version].Rejected = true
		}
	}

	m.rejected += count
	if m.rejected == uint32(len(m.versions)) {
		for version := range m.versions {
			m.versions[version].Rejected = false
		}
	}

	return count > 1
}

func (m *manager) Reset() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for version := range m.versions {
		m.versions[version].Rejected = false
	}
}

func (m *manager) Try(base record.Base) (record.Entry, parser.VersionDefinition, error) {
	// Lock the factory so nothing can be rejected.
	m.mutex.RLock()

	// Loop over each factory and provide a copy of the entry.
	expected := 0
	for _, factoryDefinition := range m.versions {
		if !factoryDefinition.Rejected {
			factoryDefinition.Input <- base
			expected += 1
		}
	}

	// Create a "winner" object that will be filled with "the winner" out of all the factories attempted.
	var winner *Result = nil
	for i := 0; i < expected; i += 1 {
		// Wait for a result from one of the potential factories and call it an attempt. There is no expectation
		// that results will return in any particular order.
		attempt := <-m.output

		// Keep records of all attempts for later calculations.
		versionParser := m.versions[attempt.Version]
		versionParser.Tries += 1

		// A rejected attempt
		if attempt.Rejected {
			// Check for a matching factory definition and remove it from the array of active factories. A goroutine
			// here because we hold a read lock and Reject will open a write lock. So Reject needs to wait for
			// this method to complete before completing the rejection.
			// noinspection GoDeferInLoop
			defer m.Reject(attempt.Version.Equals)

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
			// Count the error for future reference.
			versionParser.Errors += 1

			// When getting a version unmatched error, check for version and always use the most recent as the winner.
			if _, ok := attempt.Err.(parser.VersionMessageUnmatched); ok && winner.Version.Compare(attempt.Version) < 0 {
				winner = &attempt
			}

			util.Debug("# err version (%s)", attempt.Version)
		}
	}

	// Check for a blank winner, meaning no versions succeeded in the attempt.
	if winner == nil {
		winner = &Result{Err: parser.ErrorVersionUnmatched{}}
	} else {
		m.versions[winner.Version].Wins += 1
	}

	// Unlock the read lock. Note that this is not deferred because doing so would conflict with any deferred rejections.
	m.mutex.RUnlock()

	// Mark the winning version and return the results.
	return winner.Entry, winner.Version, winner.Err
}

func newManager(worker func(record.Base, parser.VersionParser) (record.Entry, error), parsers []parser.VersionParser) manager {
	set := make(map[parser.VersionDefinition]*logParser)

	m := manager{
		versions:  set,
		rejected:  0,
		mutex:     sync.RWMutex{},
		output:    make(chan Result, len(set)),
		waitGroup: sync.WaitGroup{},
	}

	for _, item := range parsers {
		version := item.Version()
		set[version] = &logParser{
			Input:    make(chan record.Base),
			Parser:   item,
			Rejected: false,
			Tries:    0,
			Version:  version,
			Wins:     0,
		}

		// Create a goroutine that will continuously monitor for base objects and begin conversion once received.
		go parseByVersion(set[version].Input, m.output, worker, *set[version], &m.waitGroup)
	}

	return m
}

func parseByVersion(baseIn <-chan record.Base, entryOut chan<- Result, worker func(record.Base, parser.VersionParser) (record.Entry, error), v logParser, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	// Continuously loop over the input channel to process log.Base objects as they arrive.
	result := Result{Version: v.Version}
	for base := range baseIn {
		// Do a quick-and-dirty version check and only process against factories that may return a result.
		result.Rejected = !v.Parser.Check(base)
		if !result.Rejected {
			// Run the parser against the active factory (parser).
			entry, err := worker(base, v.Parser)

			switch err.(type) {
			case parser.VersionDateUnmatched, parser.ErrorVersionUnmatched:
				result.Rejected = true
			default:
				result.Entry = entry
				result.Err = err
			}
		}
		// Create a result object, complete with version, entry result, and errors.
		entryOut <- result
	}
}
