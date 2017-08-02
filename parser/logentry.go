package parser

import (
	"fmt"
	"mgotools/util"
	"strings"
	"time"
)

// Log examples:
// Sat Jul 29 16:51:28.392 [initandlisten] db version v2.4.14
// 2017-07-29T16:53:40.671-0700 [initandlisten] db version v2.6.12
// 2017-07-29T16:55:33.242-0700 I CONTROL  [initandlisten] db version v3.0.15
// 2017-07-29T17:01:15.835-0700 I CONTROL  [initandlisten] db version v3.2.12

type RawLogEntry struct {
	Raw          string
	RawDate      string
	RawSeverity  string
	RawComponent string
	RawContext   string
	RawMessage   string
}

type LogEntry struct {
	RawLogEntry

	Date            time.Time
	DateYearMissing bool
	DateRollover    int
	DateValid       bool

	Valid bool
}

func NewLogEntry(rawLogEntry *RawLogEntry) *LogEntry {
	var (
		date  time.Time
		ok    bool
		valid bool
	)

	if date, ok = util.DateParse(rawLogEntry.RawDate); !ok {
		valid = false
	}

	return &LogEntry{
		RawLogEntry: *rawLogEntry,

		Date:            date,
		DateYearMissing: false,
		DateRollover:    0,
		DateValid:       ok,

		Valid: valid,
	}
}

// Generate a LogEntry from a line of text. This method assumes the entry is *not* JSON.
func NewRawLogEntry(line string) *RawLogEntry {
	var (
		count  int
		parts  []string = strings.Split(line, " ")
		entry           = RawLogEntry{Raw: line}
		offset int      = 0
	)

	if len(parts) < 2 {
		fmt.Println("Not enough parts to justify a line")
		return nil
	}

	// Check for a day in the first portion of the string, which represents version <= 2.4
	if util.IsDay(parts[0]) {
		_, parts = util.DateStringFromArray(parts)
	}

	parts = util.ArrayFilter(parts, func(s string) bool { return s != "" })
	if count = len(parts); count == 0 {
		return nil
	}

	entry.RawDate = parts[0]

	if count > 1 {
		if IsSeverity(parts[1]) {
			entry.RawSeverity = parts[1]
			offset += 1
		} else if IsComponent(parts[1]) {
			entry.RawComponent = parts[1]
		} else if IsContext(parts[1]) {
			entry.RawContext = parts[1]
		} else {
			entry.RawMessage = strings.Join(parts[1:], " ")
		}
	}

	if count > 2 {
		if IsComponent(parts[2]) {
			entry.RawComponent = parts[2]
		} else if IsContext(parts[2]) {
			entry.RawContext = parts[2]
		} else {
			entry.RawMessage = strings.Join(parts[2:], " ")
		}
	}

	if count > 3 {
		if IsContext(parts[3]) {
			entry.RawContext = parts[3]
		} else {
			entry.RawMessage = strings.Join(parts[3:], " ")
		}
	}

	if count > 4 && entry.RawMessage == "" {
		entry.RawMessage = strings.Join(parts[4:], " ")
	}

	return &entry
}

func IsComponent(value string) bool {
	return util.ArrayMatchString(COMPONENTS, value)
}

func IsContext(value string) bool {
	length := util.StringLength(value)
	return length > 2 && value[0] == '[' && value[length-1] == ']'
}

func IsSeverity(value string) bool {
	return util.StringLength(value) == 1 && util.ArrayMatchString(SEVERITIES, value)
}
