package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version34Parser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version34Parser{VersionCommon{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: ErrorVersionUnmatched{Message: "version 3.4"},
		}}
	})
}

func (v *Version34Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version34Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return v.parse3XCommonMessage(entry, v.ErrorVersion)
}

func (v *Version34Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 4, Binary: record.BinaryMongod}
}
func (v *Version34Parser) isExpectedComponent(c string) bool {
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
		"TRACKING",
		"WRITE",
		"-":
		return true
	default:
		return false
	}
}
