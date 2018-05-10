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
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *Version30SParser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version30SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}
func (v *Version30SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 0, Binary: record.BinaryMongos}
}
