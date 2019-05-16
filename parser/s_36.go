package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version36SParser struct{}

func init() {
	version.Factory.Register(func() version.Parser {
		return &Version36SParser{}
	})
}

var errorVersion36SUnmatched = internal.VersionUnmatched{Message: "mongos 3.6"}

func (v *Version36SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version36SParser) NewLogMessage(entry record.Entry) (message.Message, error) {
	return nil, errorVersion36SUnmatched
}
func (v *Version36SParser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 6, Binary: record.BinaryMongos}
}
