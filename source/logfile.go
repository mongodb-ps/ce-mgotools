package source

import (
	"bufio"
	"compress/gzip"
	"errors"
	"io"
	"unicode"

	"mgotools/cmd"
	"mgotools/record"
	"mgotools/util"
)

var ErrorParsingDate = errors.New("unrecognized date format")
var ErrorMissingContext = errors.New("missing context")

type logfile struct {
	Closer  io.Closer
	Line    uint
	Scanner *bufio.Scanner

	closed bool
}

// Enforce the interface at compile time.
var _ cmd.BaseFactory = (*logfile)(nil)

func NewLogfile(closer io.ReadCloser) (*logfile, error) {
	reader := bufio.NewReader(closer)
	if scanner, err := checkGZip(reader, bufio.NewScanner(closer)); err != nil {
		return nil, err
	} else {
		return &logfile{
			Closer:  closer,
			Scanner: scanner,
		}, nil
	}
}

func checkGZip(reader *bufio.Reader, scanner *bufio.Scanner) (*bufio.Scanner, error) {
	if peek, err := reader.Peek(2); err == nil {
		if peek[0] == 0x1f && peek[1] == 0x8b {
			if gzipReader, err := gzip.NewReader(reader); err == nil {
				scanner = bufio.NewScanner(gzipReader)
			} else {
				return nil, err
			}
		}
	}
	return scanner, nil
}

// Generate an Entry from a line of text. This method assumes the entry is *not* JSON.
func newBase(line string, num uint) (record.Base, error) {
	var (
		base = record.Base{RuneReader: util.NewRuneReader(line), LineNumber: num, RawSeverity: 0}
		pos  int
	)
	// Check for a day in the first portion of the string, which represents version <= 2.4
	if day := base.PreviewWord(1); util.IsDay(day) {
		base.RawDate = base.ParseCDateString()
		base.CString = true
	} else if base.IsIsoString() {
		base.RawDate, _ = base.SlurpWord()
		base.CString = false
	}
	if base.EOL() || base.RawDate == "" {
		return base, ErrorParsingDate
	}
	if base.ExpectRune('[') {
		// the context is first so assume the line remainder is the message
		if r, err := base.EnclosedString(']', false); err == nil {
			base.RawContext = r
		}
		for base.Expect(unicode.Space) {
			base.Next()
		}
	} else {
		// the context isn't first so there is likely more available to check
		for i := 0; i < 4; i += 1 {
			if part, ok := base.SlurpWord(); ok {
				if base.RawSeverity == record.SeverityNone && record.IsSeverity(part) {
					base.RawSeverity = record.Severity(part[0])
					continue
				} else if base.RawComponent == "" && record.IsComponent(part) {
					base.RawComponent = part
					continue
				} else if base.RawContext == "" && part[0] == '[' {
					base.RewindSlurpWord()
					if r, err := base.EnclosedString(']', false); err == nil {
						base.RawContext = r
						continue
					}
				}
				base.RewindSlurpWord()
				break
			}
		}
	}

	// All log entries for all supported versions have a context.
	if base.RawContext == "" {
		return base, ErrorMissingContext
	}

	pos = base.Pos()
	base.RawMessage = base.Remainder()
	base.Seek(pos, 0)
	return base, nil
}

func (f *logfile) Close() error {
	f.closed = true
	return f.Closer.Close()
}

func (f *logfile) Read() (record.Base, error) {
	if !f.closed && f.Scanner.Scan() {
		f.Line += 1
		return newBase(f.Scanner.Text(), f.Line)
	}
	return record.Base{}, io.EOF
}
