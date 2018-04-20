package parser

import (
	"mgotools/log"
	"mgotools/util"
)

type LogVersion26SParser struct {
	LogVersionCommon
}

func init() {
	LogVersionParserFactory.Register(func() LogVersionParser {
		return &LogVersion26SParser{LogVersionCommon{
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *LogVersion26SParser) NewLogMessage(entry log.Entry) (log.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}
func (v *LogVersion26SParser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major: 2, Minor: 6, Binary: LOG_VERSION_MONGOS}
}
