package parser

import (
	"mgotools/log"
	"mgotools/util"
)

type LogVersion32Parser struct {
	LogVersionCommon
}

func init() {
	LogVersionParserFactory.Register(func() LogVersionParser {
		return &LogVersion32Parser{LogVersionCommon{util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL})}}
	})
}

func (v *LogVersion32Parser) NewLogMessage(entry log.Entry) (log.Message, error) {
	r := *util.NewRuneReader(entry.RawMessage)
	switch entry.RawComponent {
	case "COMMAND", "WRITE":
		if msg, err := parse3XCommand(r, true); err == nil {
			return msg, err
		} else if msg, err := v.ParseDDL(r, entry); err == nil {
			return msg, nil
		}
	case "INDEX":
		if r.ExpectString("build index on") {
			return parse3XBuildIndex(r)
		}
	case "CONTROL":
		return v.ParseControl(r, entry)
	case "NETWORK":
		return v.ParseNetwork(r, entry)
	case "STORAGE":
		return v.ParseStorage(r, entry)
	}
	return nil, LogVersionErrorUnmatched{Message: "version 3.2"}
}
func (v *LogVersion32Parser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major: 3, Minor: 2, Binary: LOG_VERSION_MONGOD}
}
