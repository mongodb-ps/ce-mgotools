package parser

import (
	"mgotools/log"
	"mgotools/util"
)

type LogVersion34SParser struct {
	LogVersionCommon
}

func init() {
	LogVersionParserFactory.Register(func() LogVersionParser {
		return &LogVersion34SParser{LogVersionCommon{
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *LogVersion34SParser) NewLogMessage(entry log.Entry) (log.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}
func (v *LogVersion34SParser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major: 3, Minor: 4, Binary: LOG_VERSION_MONGOS}
}
