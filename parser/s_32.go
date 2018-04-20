package parser

import (
	"mgotools/log"
	"mgotools/util"
)

type LogVersion32SParser struct {
	LogVersionCommon
}

func init() {
	LogVersionParserFactory.Register(func() LogVersionParser {
		return &LogVersion32SParser{LogVersionCommon{
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *LogVersion32SParser) NewLogMessage(entry log.Entry) (log.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}
func (v *LogVersion32SParser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major: 3, Minor: 2, Binary: LOG_VERSION_MONGOS}
}
