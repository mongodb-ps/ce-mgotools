package parser

import (
	"mgotools/parser/errors"
	"mgotools/record"
	"mgotools/util"
)

type Version36SParser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version36SParser{VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: errors.VersionUnmatched{"mongos 3.6"},
		}}
	})
}

func (v *Version36SParser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version36SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry, v.ErrorVersion)
}
func (v *Version36SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 6, Binary: record.BinaryMongos}
}
