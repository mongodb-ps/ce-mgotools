package parser

import (
	"mgotools/parser/errors"
	"mgotools/record"
	"mgotools/util"
)

type Version24SParser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version24SParser{VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: errors.VersionUnmatched{"mongos 2.4"},
		}}
	})
}

func (v *Version24SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry, v.ErrorVersion)
}

func (v *Version24SParser) Check(base record.Base) bool {
	return base.CString &&
		base.RawSeverity == record.SeverityNone &&
		base.RawComponent == ""
}

func (v *Version24SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 4, Binary: record.BinaryMongos}
}
