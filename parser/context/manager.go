package context

import (
	"fmt"
	"sync"
	"sync/atomic"

	"mgotools/internal"
	"mgotools/parser"
	"mgotools/record"
)

type Result struct {
	Entry   record.Entry
	Err     error
	Version parser.VersionDefinition

	Rejected bool
}

type managerParser struct {
	sync.RWMutex

	Errors  uint64
	Input   chan managerInput
	Parser  parser.VersionParser
	Version parser.VersionDefinition

	Rejected bool
	sticky   bool
}

type managerInput struct {
	Base   record.Base
	Output chan<- Result
}

type manager struct {
	sync.RWMutex

	rejected uint32
	versions map[parser.VersionDefinition]*managerParser

	finished  bool
	waitGroup sync.WaitGroup
}

func newManager(worker func(record.Base, parser.VersionParser) (record.Entry, error), parsers []parser.VersionParser) *manager {
	set := make(map[parser.VersionDefinition]*managerParser)

	m := manager{
		RWMutex: sync.RWMutex{},

		versions: set,
		rejected: 0,

		finished:  false,
		waitGroup: sync.WaitGroup{},
	}

	for _, item := range parsers {
		version := item.Version()

		parser := &managerParser{
			RWMutex:  sync.RWMutex{},
			Input:    make(chan managerInput),
			Parser:   item,
			Rejected: false,
			Version:  version,
		}

		set[version] = parser

		// Increment the wait group.
		m.waitGroup.Add(1)

		// Create a goroutine that will continuously monitor for base objects and begin conversion once received.
		go parseByVersion(parser.Input, worker, version, parser.Parser, &m.waitGroup)
	}

	return &m
}

func parseByVersion(baseIn <-chan managerInput, worker func(record.Base, parser.VersionParser) (record.Entry, error), version parser.VersionDefinition, parser parser.VersionParser, wg *sync.WaitGroup) {
	// Continuously loop over the input channel to process log.Base objects as they arrive.
	result := Result{Version: version}
	for input := range baseIn {
		// Do a quick-and-dirty version check and only process against factories that may return a result.
		result.Rejected = !parser.Check(input.Base)
		if !result.Rejected {
			// Run the parser against the active factory (parser).
			entry, err := worker(input.Base, parser)

			if _, ok := err.(internal.VersionUnmatched); ok {
				result.Rejected = true
			} else if err == internal.VersionDateUnmatched || err == internal.VersionMessageUnmatched {
				result.Rejected = true
			} else {
				result.Entry = entry
				result.Err = err
			}
		}
		// Create a result object, complete with version, entry result, and errors.
		input.Output <- result
	}

	wg.Done()
}

func (m *manager) Finish() {
	// Iterate through each version to close the channels causing the parser
	// method to exit.
	for _, version := range m.versions {
		// Lock the version to prevent editing.
		version.Lock()

		// Close the input channel so the method will exit.
		close(version.Input)

		// Unlock the version.
		version.Unlock()
	}

	// Wait for all instances to exit before returning.
	m.waitGroup.Wait()

	m.Lock()
	defer m.Unlock()
	m.finished = true
}

// Given a version definition, return if the version has been rejected,
// and whether it exists.
func (m *manager) IsRejected(c parser.VersionDefinition) (rejected bool, found bool) {
	for definition, parser := range m.versions {
		if c.Equals(definition) {
			// Lock the object for reading.
			parser.RLock()

			// Set the appropriate variables for return.
			rejected, found = parser.Rejected, true

			// Unlock the object and return.
			parser.RUnlock()
			return
		}
	}

	// Neither found nor rejected.
	return false, false
}

// An internal method for rejecting a version definition parser. The sticky
// attribute will prevent the version from resetting later unless no other
// versions are available to Try().
func (m *manager) reject(sticky bool, check func(parser.VersionDefinition) bool) bool {
	// The `rejected` variable will be checked so the read mutex will be set
	// for the duration of this method.
	m.RLock()
	defer m.RUnlock()

	// Iterate through each version and run it against the `check()` method.
	// Any versions that should be rejected will be marked appropriately.
	for definition, parser := range m.versions {
		if check(definition) {
			// Lock the version definition so it cannot be read or modified.
			parser.Lock()

			// Mark the version as rejected and decrement the global count.
			parser.Rejected = true
			parser.sticky = sticky
			m.rejected += 1

			// Unlock the current version definition.
			parser.Unlock()
		}
	}

	// If all versions are rejected, there is a problem. Each version should
	// be "un-rejected" excluding those marked "sticky."
	if m.rejected == uint32(len(m.versions)) {
		for _, parser := range m.versions {
			// Lock the version so it cannot be modified.
			parser.Lock()

			// Check whether the rejected status should be "sticky," i.e. the
			// version definition was rejected externally.
			if !parser.sticky {
				// Clear the rejected flag and continue forward.
				parser.Rejected = false

				// Reduce the global rejected count.
				m.rejected -= 1
			}

			// Unlock the version definition.
			parser.Unlock()
		}
	}

	// Return true when at least one node has been rejected.
	return m.rejected > 1
}

// Removes a version from the list of available. New calls to Try() will avoid
// checking rejected versions.
func (m *manager) Reject(check func(parser.VersionDefinition) bool) bool {
	// The assumption is that an external Reject() should be sticky since it
	// did not result from a parsing failure. An example of this might be
	// a version line appearing in the log file.
	return m.reject(true, check)
}

// Reset all the version definitions, clearing both the rejected status and
// any sticky values. This method may be useful after a server restart.
func (m *manager) Reset() {
	// Lock the manager because the rejected count will be reset.
	m.Lock()
	defer m.Unlock()

	for _, parser := range m.versions {
		// Lock the version so it cannot be modified.
		parser.Lock()

		// Reset the rejected and sticky values.
		parser.Rejected = false
		parser.sticky = false

		// Unlock the version.
		parser.Unlock()
	}

	// Reset the rejected count to zero.
	m.rejected = 0
}

func (m *manager) Try(base record.Base) (record.Entry, parser.VersionDefinition, error) {
	// Create a local output channel for each Try(). This
	output := make(chan Result)
	expected := m.send(base, output)

	if expected == 0 {
		panic(fmt.Sprintf("no versions to try at line %d", base.LineNumber))
	}

	// Create a "winner" object that will be filled with "the winner" out of all the factories attempted.
	var winner *Result = nil
	for i := 0; i < expected; i += 1 {
		// Wait for a result from one of the potential factories and call it an attempt. There is no expectation
		// that results will return in any particular order.
		attempt := <-output

		// A rejected attempt
		if attempt.Rejected {
			// Check for a matching factory definition and remove it from the array of active factories. A goroutine
			// here because we hold a read lock and Reject will open a write lock. So Reject needs to wait for
			// this method to complete before completing the rejection.
			// noinspection GoDeferInLoop
			m.reject(false, attempt.Version.Equals)

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
			// Grab the version parser to increment the errors count.
			versionParser := m.versions[attempt.Version]

			// Count the error for future reference (but do it atomically).
			atomic.AddUint64(&versionParser.Errors, 1)

			// When getting a version unmatched error, check for version and always use the most recent as the winner.
			if attempt.Err == internal.VersionMessageUnmatched && winner.Version.Compare(attempt.Version) < 0 {
				winner = &attempt
			}
		}
	}

	// Check for a blank winner, meaning no versions succeeded in the attempt.
	if winner == nil {
		winner = &Result{Err: internal.VersionUnmatched{}}
	}

	// Mark the winning version and return the results.
	return winner.Entry, winner.Version, winner.Err
}

func (m *manager) send(base record.Base, out chan<- Result) (expected int) {
	// Lock the manager for reads because the rejected list may have changed.
	m.Lock()
	defer m.Unlock()

	// Loop over each factory and provide a copy of the entry.
	expected = 0
	for _, factoryDefinition := range m.versions {
		if !factoryDefinition.Rejected {
			factoryDefinition.Input <- managerInput{base, out}
			expected += 1
		}
	}
	return
}
