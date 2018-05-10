package record

import (
	"strings"
	"unicode"

	"mgotools/mongo"
	"mgotools/util"

	"github.com/pkg/errors"
)

type Severity rune
type Binary uint32

const SeverityNone = Severity(0)

const (
	BinaryAny = Binary(iota)
	BinaryMongod
	BinaryMongos
)

type Base struct {
	*util.RuneReader

	CString      bool
	LineNumber   uint
	RawDate      string
	RawComponent string
	RawContext   string
	RawMessage   string
	RawSeverity  Severity
}

var ErrorParsingDate = errors.New("unrecognized date format")
var ErrorMissingContext = errors.New("missing context")

// Generate an Entry from a line of text. This method assumes the entry is *not* JSON.
func NewBase(line string, num uint) (Base, error) {
	var (
		entry = Base{RuneReader: util.NewRuneReader(line), LineNumber: num, RawSeverity: 0}
		pos   int
	)
	// Check for a day in the first portion of the string, which represents version <= 2.4
	if day := entry.PreviewWord(1); util.IsDay(day) {
		entry.RawDate = entry.parseCDateString()
		entry.CString = true
	} else if entry.isISOString() {
		entry.RawDate, _ = entry.SlurpWord()
		entry.CString = false
	}
	if entry.EOL() || entry.RawDate == "" {
		return entry, ErrorParsingDate
	}
	if entry.Expect('[') {
		// the context is first so assume the line remainder is the message
		if part, _ := entry.SlurpWord(); IsContext(part) {
			entry.RawContext = part
		}
	} else {
		// the context isn't first so there is likely more available to check
		for i := 0; i < 4; i += 1 {
			if part, ok := entry.SlurpWord(); ok {
				if entry.RawSeverity == SeverityNone && IsSeverity(part) {
					entry.RawSeverity = Severity(part[0])
				} else if entry.RawComponent == "" && IsComponent(part) {
					entry.RawComponent = part
				} else if entry.RawContext == "" && IsContext(part) {
					entry.RawContext = part
				} else {
					entry.RewindSlurpWord()
					break
				}
			}
		}
	}

	// All log entries for all supported versions have a context.
	if entry.RawContext == "" {
		return entry, ErrorMissingContext
	}

	pos = entry.Pos()
	entry.RawMessage = entry.Remainder()
	entry.Seek(pos, 0)
	return entry, nil
}

func (r *Base) isISOString() bool {
	// 0000-00-00T00:00:00
	date := []rune(r.PreviewWord(1))
	length := len(date)

	return length >= 19 &&
		unicode.IsNumber(date[0]) &&
		unicode.IsNumber(date[1]) &&
		unicode.IsNumber(date[2]) &&
		unicode.IsNumber(date[3]) &&
		date[4] == '-' &&
		unicode.IsNumber(date[5]) &&
		unicode.IsNumber(date[6]) &&
		date[7] == '-' &&
		unicode.IsNumber(date[8]) &&
		unicode.IsNumber(date[9]) &&
		date[10] == 'T' &&
		unicode.IsNumber(date[11]) &&
		unicode.IsNumber(date[12]) &&
		date[13] == ':' &&
		unicode.IsNumber(date[14]) &&
		unicode.IsNumber(date[15]) &&
		date[16] == ':' &&
		unicode.IsNumber(date[17]) &&
		unicode.IsNumber(date[18])
}

// Take a parts array ([]string { "Sun", "Jan", "02", "15:04:05" }) and combined into a single element
// ([]string { "Sun Jan 02 15:04:05" }) with all trailing elements appended to the array.
func (r *Base) parseCDateString() string {
	var (
		ok     = true
		target = make([]string, 4)
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

// IsComponent checks a string value against the possible components array.
func IsComponent(value string) bool {
	return util.ArrayMatchString(mongo.COMPONENTS, value)
}

// IsContext checks for a bracketed string ([<string>])
func IsContext(value string) bool {
	length := util.StringLength(value)
	return length > 2 && value[0] == '[' && value[length-1] == ']'
}

// IsSeverity checks a string value against the severities array.
func IsSeverity(value string) bool {
	return util.StringLength(value) == 1 && util.ArrayMatchString(mongo.SEVERITIES, value)
}
