package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version30Parser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version30Parser{VersionCommon{util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL})}}
	})
}

func (v *Version30Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	if m, err := v.parse3XComponent(entry); err == nil {
		if n, ok := m.(record.MsgOpCommand); ok {
			return NormalizeCommand(n.MsgOpCommandBase), nil
		}
		return m, nil
	}
	return nil, VersionErrorUnmatched{"version 3.0"}
}

func (v *Version30Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		v.isExpectedComponent(base.RawComponent)
}

func (v *Version30Parser) isExpectedComponent(c string) bool {
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
