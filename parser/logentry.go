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

	ISO8601 bool
}

type LogEntry struct {
	Date      time.Time
	DateValid bool
}

func NewLogEntry(rawLogEntry *RawLogEntry) *LogEntry {
	fmt.Println("Attempting to parse date: ", rawLogEntry.RawDate)
	date, ok := util.DateParse(rawLogEntry.RawDate)

	fmt.Println(fmt.Sprintf("Date: %s, OK: %s", date, ok))
	return &LogEntry{
		Date:      date,
		DateValid: ok,
	}
}

// Generate a LogEntry from a line of text. This method assumes the entry is *not* JSON.
func NewRawLogEntry(line string) *RawLogEntry {
	var parts []string = strings.Split(line, " ")

	if len(parts) < 5 {
		fmt.Println("Not enough parts to justify a line")
		return nil
	}

	// Check for a day in the first portion of the string, which represents version <= 2.4
	if util.IsDay(parts[0]) {
		_, parts = util.DateStringFromArray(parts)
	}

	entry := RawLogEntry{
		Raw:          line,
		RawDate:      parts[0],
		RawSeverity:  parts[1],
		RawComponent: parts[2],
		RawContext:   parts[3],
		RawMessage:   strings.Join(parts[4:], " "),
	}

	return &entry
}
