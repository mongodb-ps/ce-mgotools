package parser

import (
	"mgotools/log"
	"mgotools/util"
)

type LogVersion24SParser struct {
	LogVersionCommon
}

func init() {
	LogVersionParserFactory.Register(func() LogVersionParser {
		return &LogVersion24SParser{LogVersionCommon{
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *LogVersion24SParser) NewLogMessage(entry log.Entry) (log.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}
func (v *LogVersion24SParser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major: 2, Minor: 4, Binary: LOG_VERSION_MONGOS}
}
