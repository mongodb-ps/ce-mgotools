package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version26SParser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version26SParser{VersionCommon{
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *Version26SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}
func (v *Version26SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 6, Binary: LOG_VERSION_MONGOS}
}
