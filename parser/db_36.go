package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version36Parser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version36Parser{VersionCommon{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: ErrorVersionUnmatched{Message: "version 3.6"},
		}}
	})
}

func (v *Version36Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version36Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return v.parse3XCommonMessage(entry, v.ErrorVersion)
}

func (v *Version36Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 6, Binary: record.BinaryMongod}
}
func (v *Version36Parser) isExpectedComponent(c string) bool {
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
		"HEARTBEATS",
		"INDEX",
		"JOURNAL",
		"NETWORK",
		"QUERY",
		"REPL",
		"REPL_HB",
		"REPLICATION",
		"ROLLBACK",
		"SHARDING",
		"STORAGE",
		"TOTAL",
		"TRACKING",
		"WRITE",
		"-":
		return true
	default:
		return false
	}
}
