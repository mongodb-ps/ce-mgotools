package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version24SParser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version24SParser{VersionCommon{
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *Version24SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}

func (v *Version24SParser) Check(base record.Base) bool {
	return base.CString &&
		base.RawSeverity == record.SeverityNone &&
		base.RawComponent == ""
}

func (v *Version24SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 4, Binary: record.BinaryMongos}
}
