package parser

import (
	"mgotools/record"
	"mgotools/util"
)

type Version32SParser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version32SParser{VersionCommon{
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *Version32SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}
func (v *Version32SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 2, Binary: LOG_VERSION_MONGOS}
}
