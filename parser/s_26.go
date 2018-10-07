package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version26SParser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version26SParser{VersionCommon{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: ErrorVersionUnmatched{"mongos 2.6"},
		}}
	})
}

func (v *Version26SParser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity == record.SeverityNone &&
		base.RawComponent == ""
}

func (v *Version26SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry, v.ErrorVersion)
}
func (v *Version26SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 6, Binary: record.BinaryMongos}
}
