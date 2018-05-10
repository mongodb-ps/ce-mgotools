package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version36SParser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version36SParser{VersionCommon{
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *Version36SParser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version36SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}
func (v *Version36SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 6, Binary: record.BinaryMongos}
}
