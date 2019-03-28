package parser

import (
	"mgotools/internal"
	"mgotools/record"
)

type Version26SParser struct{}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version26SParser{}
	})
}

var errorVersion26SUnmatched = internal.VersionUnmatched{Message: "mongos 2.6"}

func (v *Version26SParser) Check(base record.Base) bool {
	return base.CString &&
		base.RawSeverity == record.SeverityNone &&
		base.RawComponent == ""
}

func (v *Version26SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return nil, errorVersion26SUnmatched
}
func (v *Version26SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 6, Binary: record.BinaryMongos}
}
