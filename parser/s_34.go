package parser

import (
	"mgotools/internal"
	"mgotools/record"
	"mgotools/util"
)

type Version34SParser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version34SParser{VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: internal.VersionUnmatched{"mongos 3.4"},
		}}
	})
}

func (v *Version34SParser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version34SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry, v.ErrorVersion)
}

func (v *Version34SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 4, Binary: record.BinaryMongos}
}
