package parser

import (
	"time"

	"mgotools/record"
	"mgotools/util"
)

/*
 * VersionParserFactory
 */

// Global VersionParserFactory to register different version files.
var VersionParserFactory = &logVersionParserFactory{factories: make([]VersionParser, 0, 64)}

type VersionParser interface {
	Check(base record.Base) bool
	NewLogMessage(record.Entry) (record.Message, error)
	ParseDate(string) (time.Time, error)
	Version() VersionDefinition
}

type logVersionParserFactory struct {
	factories []VersionParser
}

func (f *logVersionParserFactory) GetAll() []VersionParser {
	return f.factories
}
func (f *logVersionParserFactory) Register(init func() VersionParser) {
	f.factories = append(f.factories, init())
}

/*
 * VersionDefinition
 */
type VersionDefinition struct {
	Major  int
	Minor  int
	Binary record.Binary
}

// Compares two versions. a < b == -1, a > b == 1, a = b == 0
func (a *VersionDefinition) Compare(b VersionDefinition) int {
	switch {
	case a.Major == b.Major && a.Minor == b.Minor:
		return 0
	case a.Major < b.Major,
		a.Major == b.Major && a.Minor < b.Minor:
		return -1
	case a.Major > b.Major,
		a.Major == b.Major && a.Minor > b.Minor:
		return 1
	}
	panic("version comparison failed")
}

func (a *VersionDefinition) Equals(b VersionDefinition) bool {
	return a.Compare(b) == 0 && a.Binary == b.Binary
}

func (v VersionDefinition) String() string {
	var dst [12]byte
	offset := 0

	switch v.Binary {
	case record.BinaryMongod:
		dst = [12]byte{'m', 'o', 'n', 'g', 'o', 'd', ' ', 0, '.', '.'}
	case record.BinaryMongos:
		dst = [12]byte{'m', 'o', 'n', 'g', 'o', 's', ' ', 0, '.', '.'}
	default:
		panic("unexpected binary")
	}

	if v.Major < 10 {
		dst[7] = byte(v.Major) + 0x30
	} else if v.Major < 100 {
		offset = 1
		dst[7] = byte(v.Major/10) + 0x30
		dst[8] = byte(v.Major%10) + 0x30
	} else {
		panic("version too large")
	}

	if v.Minor < 10 {
		dst[9+offset] = byte(v.Minor) + 0x30
	} else if v.Minor < 100 {
		dst[9+offset] = byte(v.Minor/10) + 0x30
		dst[10+offset] = byte(v.Minor%10) + 0x30
		offset = 2
	} else {
		panic("version too large")
	}
	return string(dst[:12-2+offset])
}

type VersionCommon struct {
	*util.DateParser
}

func (v *VersionCommon) NewLogMessage(entry record.Entry) (record.Message, error) {
	panic("unimplemented call to VersionCommon::NewLogMessage")
}
