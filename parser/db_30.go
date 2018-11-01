package parser

import (
	"mgotools/parser/errors"
	"mgotools/parser/format/modern"
	"mgotools/record"
	"mgotools/util"
)

type Version30Parser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version30Parser{VersionBaseParser: VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: errors.VersionUnmatched{"version 3.0"},
		}}
	})
}

func (v *Version30Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return modern.Message(entry, v.ErrorVersion)
}

func (v *Version30Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		v.expectedComponents(base.RawComponent)
}

func (v *Version30Parser) expectedComponents(c string) bool {
	switch c {
	case "ACCESS",
		"ACCESSCONTROL",
		"BRIDGE",
		"COMMAND",
		"CONTROL",
		"DEFAULT",
		"GEO",
		"INDEX",
		"JOURNAL",
		"NETWORK",
		"QUERY",
		"REPL",
		"REPLICATION",
		"SHARDING",
		"STORAGE",
		"TOTAL",
		"WRITE",
		"-":
		return true
	default:
		return false
	}
}

func (v *Version30Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 0, Binary: record.BinaryMongod}
}
