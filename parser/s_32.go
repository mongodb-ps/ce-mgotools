package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version32SParser struct{}

func init() {
	version.Factory.Register(func() version.Parser {
		return &Version32SParser{}
	})
}

var errorVersion32SUnmatched = internal.VersionUnmatched{"mongos 3.2"}

func (v *Version32SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.Component != record.ComponentNone
}

func (v *Version32SParser) NewLogMessage(entry record.Entry) (message.Message, error) {
	return nil, errorVersion32SUnmatched
}

func (v *Version32SParser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 2, Binary: record.BinaryMongos}
}
