package parser

import (
	"mgotools/log"
	"mgotools/util"
)

type LogVersion36SParser struct {
	LogVersionCommon
}

func init() {
	LogVersionParserFactory.Register(func() LogVersionParser {
		return &LogVersion36SParser{LogVersionCommon{
			util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
		}}
	})
}

func (v *LogVersion36SParser) NewLogMessage(entry log.Entry) (log.Message, error) {
	return logVersionSCommon.NewLogMessage(entry)
}
func (v *LogVersion36SParser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major: 3, Minor: 6, Binary: LOG_VERSION_MONGOS}
}
