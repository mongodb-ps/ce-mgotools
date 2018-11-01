package parser

import (
	"mgotools/parser/errors"
	"mgotools/parser/format/modern"
	"mgotools/record"
	"mgotools/util"
)

type Version32Parser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version32Parser{VersionBaseParser: VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: errors.VersionUnmatched{Message: "version 3.2"},
		}}
	})
}

func (v *Version32Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != "" && v.expectedComponents(base.RawComponent)
}

func (v *Version32Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return modern.Message(entry, v.ErrorVersion)
}
func (v *Version32Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 2, Binary: record.BinaryMongod}
}

func (v *Version32Parser) expectedComponents(c string) bool {
	switch c {
	case "ACCESS",
		"ACCESSCONTROL",
		"ASIO",
		"BRIDGE",
		"COMMAND",
		"CONTROL",
		"DEFAULT",
		"EXECUTOR",
		"FTDC",
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
