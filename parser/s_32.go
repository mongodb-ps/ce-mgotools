package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version32SParser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version32SParser{VersionCommon{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: ErrorVersionUnmatched{"mongos 3.2"},
		}}
	})
}

func (v *Version32SParser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version32SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry, v.ErrorVersion)
}

func (v *Version32SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 2, Binary: record.BinaryMongos}
}
