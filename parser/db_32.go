package parser

import "mgotools/util"

type LogVersion32Parser struct {
	LogVersionCommon
}

func (v *LogVersion32Parser) NewLogMessage(entry LogEntry) (LogMessage, error) {
	r := util.NewRuneReader(entry.RawMessage)
	switch entry.RawComponent {
	case "COMMAND":
		return parse3XCommand(r)
	case "INDEX":
		if r.ExpectString("build index on") {
			return parse3XBuildIndex(r)
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "version 3.2"}
}
func (v *LogVersion32Parser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major:3,Minor:2,Binary:LOG_VERSION_MONGOD}
}