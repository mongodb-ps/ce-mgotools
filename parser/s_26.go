package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version26SParser struct{}

func init() {
	version.Factory.Register(func() version.Parser {
		return &Version26SParser{}
	})
}

var errorVersion26SUnmatched = internal.VersionUnmatched{Message: "mongos 2.6"}

func (v *Version26SParser) Check(base record.Base) bool {
	return base.CString &&
		base.Severity == record.SeverityNone &&
		base.RawComponent == ""
}

func (v *Version26SParser) NewLogMessage(entry record.Entry) (message.Message, error) {
	return nil, errorVersion26SUnmatched
}
func (v *Version26SParser) Version() version.Definition {
	return version.Definition{Major: 2, Minor: 6, Binary: record.BinaryMongos}
}
