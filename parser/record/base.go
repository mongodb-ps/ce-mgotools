package record

import (
	"mgotools/internal"
)

type Severity int

const (
	SeverityNone = Severity(iota)
	SeverityD    // Debug
	SeverityD1   // Debug 1
	SeverityD2   // Debug 2
	SeverityD3   // Debug 3
	SeverityD4   // Debug 4
	SeverityD5   // Debug 5
	SeverityE    // Error
	SeverityF    // Severe/Fatal
	SeverityI    // Information/Log
	SeverityW    // Warning
)

type Binary uint32

const (
	BinaryAny = Binary(iota)
	BinaryMongod
	BinaryMongos
)

type Base struct {
	*internal.RuneReader

	CString      bool
	LineNumber   uint
	RawDate      string
	RawComponent string
	RawContext   string
	RawMessage   string
	Severity     Severity
}

func NewSeverity(s string) (Severity, bool) {
	switch s {
	case "", "-":
		return SeverityNone, true
	case "D":
		return SeverityD, true
	case "D1":
		return SeverityD1, true
	case "D2":
		return SeverityD2, true
	case "D3":
		return SeverityD3, true
	case "D4":
		return SeverityD4, true
	case "D5":
		return SeverityD5, true
	case "E":
		return SeverityE, true
	case "F":
		return SeverityF, true
	case "I":
		return SeverityI, true
	case "W":
		return SeverityW, true
	default:
		return SeverityNone, false
	}
}

func (s Severity) String() string {
	switch s {
	case SeverityNone:
		return "-"
	case SeverityD:
		return "D"
	case SeverityD1:
		return "D1"
	case SeverityD2:
		return "D2"
	case SeverityD3:
		return "D3"
	case SeverityD4:
		return "D4"
	case SeverityD5:
		return "D5"
	case SeverityE:
		return "E"
	case SeverityF:
		return "F"
	case SeverityI:
		return "I"
	case SeverityW:
		return "W"
	default:
		panic("unrecognized severity")
	}
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
