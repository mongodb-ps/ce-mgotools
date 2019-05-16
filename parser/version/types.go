package version

import (
	"strconv"

	"mgotools/parser/message"
	"mgotools/parser/record"
)

/*
 * Factory
 */

// Global Factory to register different version files.
var Factory = &factory{factories: make([]Parser, 0, 64)}

type Parser interface {
	Check(base record.Base) bool
	NewLogMessage(record.Entry) (message.Message, error)
	Version() Definition
}

type factory struct {
	factories []Parser
}

func (f *factory) GetAll() []Parser {
	return f.factories
}

func (f *factory) Register(init func() Parser) {
	f.factories = append(f.factories, init())
}

/*
 * Definition
 */
type Definition struct {
	Major  int
	Minor  int
	Binary record.Binary
}

// Compares two versions. a < b == -1, a > b == 1, a = b == 0
func (a *Definition) Compare(b Definition) int {
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

func (a *Definition) Equals(b Definition) bool {
	return a.Compare(b) == 0 && a.Binary == b.Binary
}

func (v Definition) String() string {
	var dst [12]byte
	offset := 0

	switch v.Binary {
	case record.BinaryMongod:
		dst = [12]byte{'m', 'o', 'n', 'g', 'o', 'd', ' ', 0, '.', '.'}
	case record.BinaryMongos:
		dst = [12]byte{'m', 'o', 'n', 'g', 'o', 's', ' ', 0, '.', '.'}
	case record.BinaryAny:
		dst = [12]byte{'m', 'o', 'n', 'g', 'o', '?', ' ', 0, '.', '.'}
	default:
		panic("unexpected binary " + strconv.Itoa(int(v.Binary)))
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
