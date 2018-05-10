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
	r := *util.NewRuneReader(entry.RawMessage)
	switch entry.RawComponent {
	case "COMMAND", "WRITE":
		if msg, err := parse3XCommand(r, true); err == nil {
			return msg, err
		} else if msg, err := v.ParseDDL(r, entry); err == nil {
			return msg, nil
		}
	case "INDEX":
		if r.ExpectString("build index on") {
			return parse3XBuildIndex(r)
		}
	case "CONTROL":
		return v.ParseControl(r, entry)
	case "NETWORK":
		return v.ParseNetwork(r, entry)
	case "STORAGE":
		return v.ParseStorage(r, entry)
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
	case "CONTROL",
		"DEFAULT",
		"EXECUTOR",
		"GEO",
		"INDEX",
		"JOURNAL",
		"NETWORK",
		"QUERY",
		"REPL",
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
