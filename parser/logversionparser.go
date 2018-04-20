package parser

import (
	"time"

	"mgotools/log"
	"mgotools/util"
)

type LogVersionCommon struct {
	*util.DateParser
}

/*
 * LogVersionParserFactory
 */
// Global LogVersionParserFactory to register different version files.
var LogVersionParserFactory = &logVersionParserFactory{factories: make([]LogVersionParser, 0, 64)}

type LogVersionParser interface {
	NewLogMessage(log.Entry) (log.Message, error)
	ParseDate(string) (time.Time, error)
	Version() LogVersionDefinition
}
type logVersionParserFactory struct {
	count     int
	factories []LogVersionParser
}

func (f *logVersionParserFactory) Get() []LogVersionParser {
	return f.factories
}
func (f *logVersionParserFactory) Register(init func() LogVersionParser) {
	f.factories = append(f.factories, init())
}

/*
 * LogVersionDefinition
 */
type LogVersionDefinition struct {
	Major  int
	Minor  int
	Binary int
}

func (v *LogVersionDefinition) Hash() int64 {
	// This has function will clash if v.Major gets to be 32 bits long, but if that happens something has gone horribly
	// wrong in the world.
	if v.Binary == LOG_VERSION_MONGOD {
		return int64(v.Major)<<32 + int64(v.Minor)
	} else if v.Binary == LOG_VERSION_MONGOS {
		return int64(v.Major)<<32 + int64(v.Minor) | int64(1<<63-1)
	} else {
		panic("unexpected binary")
	}
}

// Compares two versions. a < b == -1, a > b == 1, a = b == 0
func (a *LogVersionDefinition) Compare(b LogVersionDefinition) int {
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

func (v LogVersionDefinition) String() string {
	var dst [12]byte
	offset := 0

	switch v.Binary {
	case LOG_VERSION_MONGOD:
		dst = [12]byte{'m', 'o', 'n', 'g', 'o', 'd', ' ', 0, '.', '.'}
	case LOG_VERSION_MONGOS:
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
