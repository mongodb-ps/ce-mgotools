package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version34SParser struct{}

var errorVersion34SUnmatched = internal.VersionUnmatched{Message: "mongos 3.4"}

func init() {
	version.Factory.Register(func() version.Parser {
		return &Version34SParser{}
	})
}

func (v *Version34SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version34SParser) NewLogMessage(entry record.Entry) (message.Message, error) {
	return nil, errorVersion34SUnmatched
}

func (v *Version34SParser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 4, Binary: record.BinaryMongos}
}
