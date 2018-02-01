package parser

import (
	"mgotools/mongo"
	"mgotools/util"
	"strconv"
	"strings"
)

type LogVersion24Parser struct {
	LogVersionCommon
}

func (v *LogVersion24Parser) NewLogMessage(entry LogEntry) (LogMessage, error) {
	r := *util.NewRuneReader(entry.RawMessage)
	switch {
	case entry.Context == "initandlisten", entry.Context == "signalProcessingThread":
		// Check for control messages, which is almost everything in 2.4 that is logged at startup.
		if msg, err := v.LogVersionCommon.ParseControl(r, entry); err == nil {
			// Most startup messages are part of control.
			return msg, nil
		} else if msg, err := v.LogVersionCommon.ParseNetwork(r, entry); err == nil {
			// Alternatively, we care about basic network actions like new connections being established.
			return msg, nil
		}
	case entry.Connection > 0:
		// Check for connection related messages, which is almost everything *not* related to startup messages.
		switch {
		case r.ExpectString("command"),
			r.ExpectString("query"),
			r.ExpectString("update"):
			// Commands or queries!
			return parse24Command(r)
		case r.ExpectString("insert"),
			r.ExpectString("getmore"),
			r.ExpectString("remove"):
			// Inserts!
			return parse24Operation(r)
		case r.ExpectString("build index"):
			// Look at things relating to indexes.
			if r.ExpectString("build index done") {
				break
			}
			return parse24BuildIndex(r)
		default:
			// Check for network connection changes.
			if msg, err := v.ParseNetwork(r, entry); err == nil {
				return msg, nil
			} else if msg, err := v.ParseDDL(r, entry); err == nil {
				return msg, nil
			}
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "version 2.4"}
}
func (v *LogVersion24Parser) Version() LogVersionDefinition {
	return LogVersionDefinition{Major: 2, Minor: 4, Binary: LOG_VERSION_MONGOD}
}
func parse24BuildIndex(r util.RuneReader) (LogMessage, error) {
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
			if msg.Properties, err = mongo.ParseJsonRunes(&r, false); err == nil {
				return msg, nil
			}
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "index format unrecognized"}
}

func parse24Command(r util.RuneReader) (LogMsgOpCommandLegacy, error) {
	var err error
	// command test.$cmd command: { getlasterror: 1.0, w: 1.0 } ntoreturn:1 keyUpdates:0  reslen:67 0ms
	// query test.foo query: { b: 1.0 } ntoreturn:0 ntoskip:0 nscanned:10 keyUpdates:0 locks(micros) r:146 nreturned:1 reslen:64 0ms
	// update vcm_audit.payload.files query: { _id: ObjectId('000000000000000000000000') } update: { _id: ObjectId('000000000000000000000000') } idhack:1 nupdated:1 upsert:1 keyUpdates:0 locks(micros) w:33688 194ms
	op := MakeLogMsgOpCommandLegacy()
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()
	var target map[string]int = op.Counters
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		if param[len(param)-1] == ':' {
			param = param[:len(param)-1]
			if op.Name == "" {
				op.Name = param
			}
			if r.Expect('{') {
				if op.Command[param], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return op, err
				}
			}
			continue
		}
		parseIntegerKeyValueErratic(param, target, &r)
		if param == "locks(micros)" {
			target = op.Locks
		}
	}
	if dur, ok := r.SlurpWord(); ok && strings.HasSuffix(dur, "ms") {
		op.Duration, _ = strconv.ParseInt(dur[0:len(dur)-3], 10, 64)
	}
	return op, nil
}
func parse24Operation(r util.RuneReader) (LogMessage, error) {
	// insert test.system.indexes ninserted:1 keyUpdates:0 locks(micros) w:10527 10ms
	op := MakeLogMsgOpCommandLegacy()
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()
	var target map[string]int = op.Counters
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		if param == "locks(micros)" {
			target = op.Locks
			continue
		} else if param == "locks:{" {
			// Wrong version, so exit.
			return nil, LogVersionErrorUnmatched{}
		}
		parseIntegerKeyValue(param, target, mongo.COUNTERS)
	}
	if dur, ok := r.SlurpWord(); ok && strings.HasSuffix(dur, "ms") {
		op.Duration, _ = strconv.ParseInt(dur[0:len(dur)-3], 10, 64)
	}
	return op, nil
}
func parseIntegerKeyValueErratic(param string, target map[string]int, r *util.RuneReader) {
	if !parseIntegerKeyValue(param, target, mongo.COUNTERS) && param[len(param)-1] == ':' {
		param = param[0 : len(param)-1]
		if num, err := strconv.ParseInt(r.PreviewWord(1), 10, 64); err == nil {
			if _, ok := mongo.COUNTERS[param]; ok {
				target[mongo.COUNTERS[param]] = int(num)
			} else {
				panic("unexpected counter type " + param + " found")
			}
		}
	}
}
