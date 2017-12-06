package parser

import "mgotools/util"

type LogVersion34Parser struct {
	LogVersionCommon
}

func (v *LogVersion34Parser) NewLogMessage(entry LogEntry) (LogMessage, error) {
	r := util.NewRuneReader(entry.RawMessage)
	switch entry.RawComponent {
	case "COMMAND":
		return parse3XCommand(r)
	case "INDEX":
		if r.ExpectString("build index on") {
			return parse3XBuildIndex(r)
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "version 3.4"}
}
func (v *LogVersion34Parser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major:3,Minor:4,Binary:LOG_VERSION_MONGOD}
}