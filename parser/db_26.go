package parser

import (
	"fmt"
	"strconv"
	"strings"

	"mgotools/mongo"
	"mgotools/record"
	"mgotools/util"

	"github.com/pkg/errors"
)

type LogVersion26Parser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &LogVersion26Parser{VersionCommon{util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL})}}
	})
}

func (v *LogVersion26Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := *util.NewRuneReader(entry.RawMessage)
	switch {
	case entry.Context == "initandlisten", entry.Context == "signalProcessingThread":
		// Check for control messages, which is almost everything in 2.6 that is logged at startup.
		if msg, err := v.VersionCommon.ParseControl(r, entry); err == nil {
			// Most startup messages are part of control.
			return msg, nil
		} else if msg, err := v.VersionCommon.ParseNetwork(r, entry); err == nil {
			// Alternatively, we care about basic network actions like new connections being established.
			return msg, nil
		}
	case entry.Connection > 0:
		switch {
		case r.ExpectString("command"):
			return parse26Command(r)
		case r.ExpectString("query"),
			r.ExpectString("getmore"),
			r.ExpectString("insert"),
			r.ExpectString("update"),
			r.ExpectString("remove"):
			// Commands, inserts, and queries!
			return parse26Operation(r)
			// Look at things related to index builds.
		case r.ExpectString("build index on"):
			return parse26BuildIndex(r)
		default:
			// Check for network status changes.
			if msg, err := v.ParseNetwork(r, entry); err == nil {
				return msg, err
			} else if msg, err := v.ParseDDL(r, entry); err == nil {
				return msg, err
			}
		}
	}
	return nil, VersionErrorUnmatched{Message: "version 2.6"}
}
func parse26BuildIndex(r util.RuneReader) (record.Message, error) {
	// 2.6 index building format is the same as 3.x
	return parse3XBuildIndex(r)
}
func parse26Command(r util.RuneReader) (record.Message, error) {
	// command test.$cmd command: insert { insert: "foo", documents: [ { _id: ObjectId('59e3fdf50bae7edf962785a7'), a: 1.0 } ], ordered: true } keyUpdates:0 numYields:0 locks(micros) w:159 reslen:40 0ms
	var err error
	op := record.MakeMsgOpCommandLegacy()
	if opn, ok := r.SlurpWord(); ok {
		op.Operation = opn
	} else {
		return nil, errors.New("operation not found")
	}
	if ns, ok := r.SlurpWord(); ok && strings.ContainsRune(ns, '.') {
		op.Namespace = ns
	} else {
		return nil, errors.New("no namespace found")
	}
	// Skip the "command:" portion, since it's irrelevant here.
	r.SkipWords(1)
	target := op.Counters
	for {
		if param, ok := r.SlurpWord(); ok {
			if r.Expect('{') {
				if op.Command[param], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return nil, err
				}
				op.Name = param
			} else if param == "locks(micros)" {
				target = op.Locks
				continue
			} else if strings.HasPrefix(param, "ms") {
				if op.Duration, err = strconv.ParseInt(param[:len(param)-2], 10, 64); err != nil {
					return nil, err
				}
			} else if !parseIntegerKeyValue(param, target, mongo.COUNTERS) {
				break
			}
		}
	}
	return op, nil
}
func parse26Operation(r util.RuneReader) (record.Message, error) {
	// getmore test.foo cursorid:30107363235 ntoreturn:3 keyUpdates:0 numYields:0 locks(micros) r:14 nreturned:3 reslen:119 0ms
	// insert test.foo query: { _id: ObjectId('5a331671de4f2a133f17884b'), a: 2.0 } ninserted:1 keyUpdates:0 numYields:0 locks(micros) w:10 0ms
	// remove test.foo query: { a: { $gte: 9.0 } } ndeleted:1 keyUpdates:0 numYields:0 locks(micros) w:63 0ms
	// update test.foo query: { a: { $gte: 8.0 } } update: { $set: { b: 1.0 } } nscanned:9 nscannedObjects:9 nMatched:1 nModified:1 keyUpdates:0 numYields:0 locks(micros) w:135 0ms
	var err error
	op := record.MakeMsgOpCommandLegacy()
	// Grab the operation name first.
	if opn, ok := r.SlurpWord(); ok {
		op.Operation = opn
	} else {
		return nil, VersionErrorUnmatched{"unexpected operation"}
	}
	if ns, ok := r.SlurpWord(); ok && strings.ContainsRune(ns, '.') {
		op.Namespace = ns
	} else {
		r.RewindSlurpWord()
	}
	target := op.Counters
	for param, ok := r.SlurpWord(); ok; param, ok = r.SlurpWord() {
		if strings.HasPrefix(param, "locks") {
			if param == "locks(micros)" {
				// Locks follow this param, so reset the target to the locks map.
				target = op.Locks
				continue
			} else {
				// Wrong version.
				return nil, VersionErrorUnmatched{}
			}
		} else if param == "planSummary:" {
			// Plan summaries require complicated and special code, so branch off and parse for plan summaries.
			if op.PlanSummary, err = parsePlanSummary(&r); err != nil {
				return nil, err
			}
			continue
		} else if length := len(param); length > 1 && util.ArrayBinarySearchString(param[:length-1], mongo.OPERATIONS) {
			if r.EOL() {
				return nil, VersionErrorUnmatched{"unexpected end of string"}
			} else if r.Expect('{') {
				// Parse JSON, found immediately after an operation.
				if op.Command, err = mongo.ParseJsonRunes(&r, false); err != nil {
					return nil, err
				}
			}
		} else if strings.ContainsRune(param, ':') {
			// A counter (in the form of key:value) needs to be applied to the correct target.
			parseIntegerKeyValue(param, target, mongo.COUNTERS)
		} else if strings.HasSuffix(param, "ms") {
			// Found a duration, which is also the last thing in the line.
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-3], 10, 64)
			break
		} else {
			// An unexpected value means that this parser either isn't the correct version or the line is invalid.
			return nil, VersionErrorUnmatched{fmt.Sprintf("encountered unexpected value '%s'", param)}
		}
	}
	return op, nil
}
func (v *LogVersion26Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 6, Binary: LOG_VERSION_MONGOD}
}
