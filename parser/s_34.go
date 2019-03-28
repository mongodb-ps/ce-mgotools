package parser

import (
	"mgotools/internal"
	"mgotools/record"
)

type Version34SParser struct{}

var errorVersion34SUnmatched = internal.VersionUnmatched{Message: "mongos 3.4"}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version34SParser{}
	})
}

func (v *Version34SParser) Check(base record.Base) bool {
	return base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version34SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return nil, errorVersion34SUnmatched
}

func (v *Version34SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 4, Binary: record.BinaryMongos}
}
