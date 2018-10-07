package parser

import (
	"strconv"
	"strings"

	"mgotools/mongo"
	"mgotools/record"
	"mgotools/util"
)

type Version24Parser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version24Parser{VersionCommon{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_CTIMENOMS, util.DATE_FORMAT_CTIME, util.DATE_FORMAT_CTIMEYEAR}),
			ErrorVersion: ErrorVersionUnmatched{Message: "version 2.4"},
		}}
	})
}

func (v *Version24Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := *util.NewRuneReader(entry.RawMessage)
	switch {
	case entry.Context == "initandlisten", entry.Context == "signalProcessingThread":
		// Check for control messages, which is almost everything in 2.4 that is logged at startup.
		if msg, err := ParseControl(r, entry); err == nil {
			// Most startup messages are part of control.
			return msg, nil
		} else if msg, err := ParseNetwork(r, entry); err == nil {
			// Alternatively, we care about basic network actions like new connections being established.
			return msg, nil
		}
	case entry.Connection > 0:
		// Check for connection related messages, which is almost everything *not* related to startup messages.
		switch {
		case r.ExpectString("query"),
			r.ExpectString("update"),
			r.ExpectString("remove"):
			// Commands or queries!
			return parse24WithPayload(r)
		case r.ExpectString("command"):
			// Commands in 2.4 don't include anything that should be converted
			// into operations (e.g. find, update, remove, etc).
			op, err := parse24WithPayload(r)
			if err != nil {
				cmd := record.MakeMsgCommandLegacy()
				cmd.Command = op.Operation
				cmd.Duration = op.Duration
				cmd.Locks = op.Locks
				cmd.Namespace = op.Namespace
				cmd.Payload = op.Payload
			}
			return nil, err
		case r.ExpectString("insert"),
			r.ExpectString("getmore"):
			// Inserts!
			return parse24WithoutPayload(r)
		case r.ExpectString("build index"):
			// Look at things relating to indexes.
			if r.ExpectString("build index done") {
				break
			}
			return parse24BuildIndex(r)
		default:
			// Check for network connection changes.
			if msg, err := ParseNetwork(r, entry); err == nil {
				return msg, nil
			}
		}
	}
	return nil, v.ErrorVersion
}

func swap(key string, m map[string]interface{}) {
	if n, ok := m["command"].(map[string]interface{}); ok {
		if _, ok := n[key]; ok {
			m[key] = m["command"]
			delete(m, "command")
		}
	}
}

func (v *Version24Parser) Check(base record.Base) bool {
	return base.RawComponent == "" &&
		base.RawSeverity == record.SeverityNone &&
		base.CString
}

func (v *Version24Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 4, Binary: record.BinaryMongod}
}
func parse24BuildIndex(r util.RuneReader) (record.Message, error) {
	// build index database.collection { key: 1.0 }
	var (
		err error
		msg record.MsgCollectionIndexOperation
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
	return nil, ErrorVersionUnmatched{Message: "index format unrecognized"}
}
func parse24WithPayload(r util.RuneReader) (record.MsgOperationLegacy, error) {
	var err error
	// command test.$cmd command: { getlasterror: 1.0, w: 1.0 } ntoreturn:1 keyUpdates:0  reslen:67 0ms
	// query test.foo query: { b: 1.0 } ntoreturn:0 ntoskip:0 nscanned:10 keyUpdates:0 locks(micros) r:146 nreturned:1 reslen:64 0ms
	// update vcm_audit.payload.files query: { _id: ObjectId('000000000000000000000000') } update: { _id: ObjectId('000000000000000000000000') } idhack:1 nupdated:1 upsert:1 keyUpdates:0 locks(micros) w:33688 194ms
	op := record.MakeMsgOperationLegacy()
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()
	var target = op.Counters
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		if param[len(param)-1] == ':' {
			param = param[:len(param)-1]
			if op.Operation == "" {
				op.Operation = param
			}
			if r.Expect('{') {
				if op.Payload[param], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return op, err
				}
			}
			// For whatever reason, numYields has a space between it and the
			// number (e.g. "numYields: 5").
			if !util.IsNumericRune(r.NextRune()) {
				continue
			} else {
				// Re-add so the parseIntegerKeyValueErratic will succeed.
				param = param + ":"
			}
		}
		parseIntegerKeyValueErratic(param, target, &r)
		if param == "locks(micros)" {
			target = op.Locks
		} else if strings.HasSuffix(param, "ms") {
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-3], 10, 64)
		}
	}
	return op, nil
}
func parse24WithoutPayload(r util.RuneReader) (record.MsgOperationLegacy, error) {
	// insert test.system.indexes ninserted:1 keyUpdates:0 locks(micros) w:10527 10ms
	op := record.MakeMsgOperationLegacy()
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()
	var target map[string]int = op.Counters
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		if param == "locks(micros)" {
			target = op.Locks
			continue
		} else if param == "locks:{" {
			// Wrong version, so exit.
			return record.MsgOperationLegacy{}, ErrorVersionUnmatched{}
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
				r.SlurpWord()
			} else {
				panic("unexpected counter type " + param + " found")
			}
		}
	}
}
