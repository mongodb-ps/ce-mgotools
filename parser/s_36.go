package parser

import (
	"mgotools/internal"
	"mgotools/record"
)

type Version36SParser struct{}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version36SParser{}
	})
}

var errorVersion36SUnmatched = internal.VersionUnmatched{Message: "mongos 3.6"}

func (v *Version36SParser) Check(base record.Base) bool {
	return base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version36SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return nil, errorVersion36SUnmatched
}
func (v *Version36SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 6, Binary: record.BinaryMongos}
}
