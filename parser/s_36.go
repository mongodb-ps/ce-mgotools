package parser

import (
	"mgotools/internal"
	"mgotools/record"
)

type Version36SParser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version36SParser{VersionBaseParser{
			ErrorVersion: internal.VersionUnmatched{"mongos 3.6"},
		}}
	})
}

func (v *Version36SParser) Check(base record.Base) bool {
	return base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version36SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry, v.ErrorVersion)
}
func (v *Version36SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 6, Binary: record.BinaryMongos}
}
