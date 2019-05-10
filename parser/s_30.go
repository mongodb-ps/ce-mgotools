package parser

import (
	"mgotools/internal"
	"mgotools/record"
)

type Version30SParser struct{}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version30SParser{}
	})
}

var errorVersion30SUnmatched = internal.VersionUnmatched{"mongos 3.0"}

func (v *Version30SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version30SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return nil, errorVersion30SUnmatched
}
func (v *Version30SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 0, Binary: record.BinaryMongos}
}
