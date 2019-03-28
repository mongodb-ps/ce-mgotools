package parser

import (
	"mgotools/internal"
	"mgotools/record"
)

type Version24SParser struct{}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version24SParser{}
	})
}

var errorVersion24SUnmatched = internal.VersionUnmatched{"mongos 2.4"}

func (v *Version24SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return nil, errorVersion24SUnmatched
}

func (v *Version24SParser) Check(base record.Base) bool {
	return base.CString &&
		base.RawSeverity == record.SeverityNone &&
		base.RawComponent == ""
}

func (v *Version24SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 4, Binary: record.BinaryMongos}
}
