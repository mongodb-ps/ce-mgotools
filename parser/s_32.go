package parser

import (
	"mgotools/internal"
	"mgotools/record"
)

type Version32SParser struct{}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version32SParser{}
	})
}

var errorVersion32SUnmatched = internal.VersionUnmatched{"mongos 3.2"}

func (v *Version32SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version32SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return nil, errorVersion32SUnmatched
}

func (v *Version32SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 2, Binary: record.BinaryMongos}
}
