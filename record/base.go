package record

import (
	"mgotools/util"
)

type Severity rune

const SeverityNone = Severity(0)

type Binary uint32

const (
	BinaryAny = Binary(iota)
	BinaryMongod
	BinaryMongos
)

type BaseFactory interface {
	Next() bool
	Get() (Base, error)
	Close() error
}

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

func (s Severity) String() string {
	if s == SeverityNone {
		s = '-'
	}
	return string(s)
}

func (b Binary) String() string {
	switch b {
	case BinaryMongod:
		return "mongod"
	case BinaryMongos:
		return "mongos"
	default:
		return "unknown"
	}
}
