package parser

import (
	"fmt"
	"github.com/pkg/errors"
	"mgotools/util"
	"strings"
)

type RawLogEntry struct {
	*util.RuneReader

	CString      bool
	RawDate      string
	RawComponent string
	RawContext   string
	RawMessage   string
	RawSeverity  string
}

// Generate a LogEntry from a line of text. This method assumes the entry is *not* JSON.
func NewRawLogEntry(line string) (RawLogEntry, error) {
	var (
		entry RawLogEntry = RawLogEntry{RuneReader: util.NewRuneReader(line)}
		pos   int
	)

	// Check for a day in the first portion of the string, which represents version <= 2.4
	if day := entry.PreviewWord(1); util.IsDay(day) {
		entry.RawDate = entry.parseCDateString()
		entry.CString = true
	} else {
		entry.RawDate, _ = entry.SlurpWord()
		entry.CString = false
	}
	if entry.EOL() || entry.RawDate == "" {
		fmt.Println("Could not parse date")
		return entry, errors.New(fmt.Sprintf("could not parse date format: %s", line))
	}

	if entry.Peek(1) == "[" {
		// the context is first so assume the line remainder is the message
		if part, _ := entry.SlurpWord(); util.IsContext(part) {
			entry.RawContext = part
		}
	} else {
		// the context isn't first so there is likely more available to check
		for i := 0; i < 4; i += 1 {
			if part, ok := entry.SlurpWord(); ok {
				if entry.RawSeverity == "" && util.IsSeverity(part) {
					entry.RawSeverity = part
				} else if entry.RawComponent == "" && util.IsComponent(part) {
					entry.RawComponent = part
				} else if entry.RawContext == "" && util.IsContext(part) {
					entry.RawContext = part
				} else {
					entry.RewindSlurpWord()
					break
				}
			}
		}
	}

	pos = entry.Pos()
	entry.RawMessage = entry.Remainder()
	entry.Seek(pos, 0)
	return entry, nil
}

// Take a parts array ([]string { "Sun", "Jan", "02", "15:04:05" }) and combined into a single element
// ([]string { "Sun Jan 02 15:04:05" }) with all trailing elements appended to the array.
func (r *RawLogEntry) parseCDateString() string {
	var (
		ok     bool     = true
		target []string = make([]string, 4)
	)
	start := r.Pos()
	for i := 0; i < 4 && ok; i++ {
		target[i], ok = r.SlurpWord()
	}

	switch {
	case !util.IsDay(target[0]):
	case !util.IsMonth(target[1]):
	case !util.IsNumeric(target[2]):
	case !util.IsTime(target[3]):
		r.Seek(start, 0)
		return ""
	}

	return strings.Join(target, " ")
}
