package parser

import (
	"mgotools/util"
	"strconv"
	"strings"
)

type LogVersion24Parser struct {
	LogVersionCommon
}

func (v *LogVersion24Parser) NewLogMessage(entry LogEntry) (LogMessage, error) {
	r := util.NewRuneReader(entry.RawMessage)
	if entry.Connection > 0 {
		switch {
		case r.ExpectString("command"):
			return parse24Command(r)
		case r.ExpectString("insert"):
			return parse24Insert(r)
		case r.ExpectString("query"):
			if op, err := parse24Command(r); err == nil {
				return nil, err
			} else {
				for i := 0; i < 2; i += 1 {
					if query, ok := op.Command["query"].(map[string]interface{}); ok {
						op.Command = query
					}
				}
			}
		case r.ExpectString("build index"):
			if r.ExpectString("build index done") {
				break
			}
			return parse24BuildIndex(r)
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "version 2.4"}
}
func (v *LogVersion24Parser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major:2,Minor:4,Binary:LOG_VERSION_MONGOD}
}
func parse24BuildIndex(r *util.RuneReader) (LogMessage, error) {
	// build index database.collection { key: 1.0 }
	var (
		err error
		msg LogMsgOpIndex
	)
	switch {
	case r.ExpectString("build index"):
		msg.Operation = "build index"
		msg.Namespace, _ = r.SkipWords(2).SlurpWord()
		if r.NextRune() == '{' {
			if msg.Properties, err = util.ParseJsonRunes(r, false); err != nil {
				return msg, nil
			}
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "index format unrecognized"}
}

func parse24Command(r *util.RuneReader) (LogMsgOpCommandLegacy, error) {
	var err error
	// command test.$cmd command: { getlasterror: 1.0, w: 1.0 } ntoreturn:1 keyUpdates:0  reslen:67 0ms
	op := LogMsgOpCommandLegacy{
		Counters: make(map[string]int),
		Locks:    make(map[string]int),
	}
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()
	if op.Command, err = util.ParseJsonRunes(r.SkipWords(1), false); err != nil {
		return LogMsgOpCommandLegacy{}, err
	}
	var target map[string]int = op.Counters
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		parseIntegerKeyValueErratic(param, target, r)
		if param == "locks(micros)" {
			target = op.Locks
		}
	}
	if dur, ok := r.SlurpWord(); ok && strings.HasSuffix(dur, "ms") {
		op.Duration, _ = strconv.ParseInt(dur[0:len(dur)-3], 10, 64)
	}
	return op, nil
}
func parseIntegerKeyValueErratic(param string, target map[string]int, r *util.RuneReader) {
	if !parseIntegerKeyValue(param, target, util.COUNTERS) && param[len(param)-1] == ':' {
		param = param[0: len(param)-1]
		if num, err := strconv.ParseInt(r.PreviewWord(1), 10, 64); err == nil {
			if _, ok := util.COUNTERS[param]; ok {
				target[util.COUNTERS[param]] = int(num)
			} else {
				panic("unexpected counter type " + param + " found")
			}
		}
	}
}
func parse24Insert(r *util.RuneReader) (LogMsgOpCommandLegacy, error) {
	// insert test.system.indexes ninserted:1 keyUpdates:0 locks(micros) w:10527 10ms
	op := LogMsgOpCommandLegacy{
		Counters: make(map[string]int),
		Locks:    make(map[string]int),
	}
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()
	var target map[string]int = op.Counters
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		if param == "locks(micros)" {
			target = op.Locks
			continue
		}
		parseIntegerKeyValue(param, target, util.COUNTERS)
	}
	if dur, ok := r.SlurpWord(); ok && strings.HasSuffix(dur, "ms") {
		op.Duration, _ = strconv.ParseInt(dur[0:len(dur)-3], 10, 64)
	}
	return op, nil
}

