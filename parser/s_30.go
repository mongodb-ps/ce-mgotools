package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version30SParser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version30SParser{VersionCommon{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: ErrorVersionUnmatched{"mongos 3.0"},
		}}
	})
}

func (v *Version30SParser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version30SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry, v.ErrorVersion)
}
func (v *Version30SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 0, Binary: record.BinaryMongos}
}
