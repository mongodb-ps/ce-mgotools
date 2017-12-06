package parser

import "mgotools/util"

type LogVersion30Parser struct {
	LogVersionCommon
}

func (v *LogVersion30Parser) NewLogMessage(entry LogEntry) (LogMessage, error) {
	r := util.NewRuneReader(entry.RawMessage)
	switch entry.RawComponent {
	case "COMMAND":
		return parse3XCommand(r)
	case "INDEX":
		if r.ExpectString("build index on") {
			return parse3XBuildIndex(r)
		}
	}
	return v.LogVersionCommon.NewLogMessage(entry)
}
func (v *LogVersion30Parser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major:3,Minor:0,Binary:LOG_VERSION_MONGOD}
}