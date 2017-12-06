package parser

import (
	"mgotools/util"
	"strconv"
	"strings"
)

type LogVersion26Parser struct {
	LogVersionCommon
}

func (v *LogVersion26Parser) NewLogMessage(entry LogEntry) (LogMessage, error) {
	r := util.NewRuneReader(entry.RawMessage)
	if entry.Connection > 0 {
		switch {
		case r.ExpectString("command"):
			return parse26Command(r)
		case r.ExpectString("build index on"):
			return parse26BuildIndex(r)
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "version 2.6"}
}
func parse26BuildIndex(r *util.RuneReader) (LogMessage, error) {
	// 2.6 index building format is the same as 3.x
	return parse3XBuildIndex(r)
}
func parse26Command(r *util.RuneReader) (LogMessage, error) {
	var err error
	util.Debug("starting parse command")
	// command test.$cmd command: insert { insert: "foo", documents: [ { _id: ObjectId('59e3fdf50bae7edf962785a7'), a: 1.0 } ], ordered: true } keyUpdates:0 numYields:0 locks(micros) w:159 reslen:40 0ms
	op := LogMsgOpCommandLegacy{}
	if params, ok := parseCommandPrefix(r); !ok {
		return nil, LogVersionErrorUnmatched{"unexpected command prefix´"}
	} else {
		op.Operation = params[0]
		util.Debug("found operation: %s", op.Operation)
		if r.Expect('{') {
			if op.Command, err = util.ParseJsonRunes(r, false); err != nil {
				return nil, err
			}
			util.Debug("parsed rune: %+v", op.Command)
			var param string
			target := op.Counters
			for param, ok = r.SlurpWord(); ok; param, ok = r.SlurpWord() {
				if param == "locks(micros)" {
					target = op.Locks
					continue
				}
				parseIntegerKeyValue(param, target, util.COUNTERS)
			}
			if dur, ok := r.SlurpWord(); ok && strings.HasSuffix(dur, "ms") {
				op.Duration, _ = strconv.ParseInt(dur[0:len(dur)-3], 10, 64)
			}
			op.Operation = strings.TrimRight(op.Operation, ":")
			return op, err
		}
	}
	util.Debug("failed to parse command")
	return nil, LogVersionErrorUnmatched{"unexpected end of string"}
}
func (v *LogVersion26Parser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major:2,Minor:6,Binary:LOG_VERSION_MONGOD}
}