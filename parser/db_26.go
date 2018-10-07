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

type Version26Parser struct {
	VersionCommon
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version26Parser{VersionCommon{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: ErrorVersionUnmatched{Message: "version 2.6"},
		}}
	})
}

func (v *Version26Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := *util.NewRuneReader(entry.RawMessage)
	switch {
	case entry.Context == "initandlisten", entry.Context == "signalProcessingThread":
		// Check for control messages, which is almost everything in 2.6 that is logged at startup.
		if msg, err := ParseControl(r, entry); err == nil {
			// Most startup messages are part of control.
			return msg, nil
		} else if msg, err := ParseNetwork(r, entry); err == nil {
			// Alternatively, we care about basic network actions like new connections being established.
			return msg, nil
		}
	case entry.Connection > 0:
		switch {
		case r.ExpectString("command"):

			c, err := parse26Command(r)
			if err != nil {
				return c, err
			}

			if crud, ok := parse26CRUD(c.Command, c.Counters, c.Payload); ok {
				c.CRUD = &crud
			}

			return c, nil

		case r.ExpectString("query"),
			r.ExpectString("getmore"),
			r.ExpectString("geonear"),
			r.ExpectString("insert"),
			r.ExpectString("update"),
			r.ExpectString("remove"):

			m, err := parse26Operation(r)
			if err != nil {
				return m, err
			}

			if crud, ok := parse26CRUD(m.Operation, m.Counters, m.Payload); ok {
				m.CRUD = &crud
			}

			return m, nil

		default:
			// Check for network status changes.
			if msg, err := ParseNetwork(r, entry); err == nil {
				return msg, err
			}
		}
	}
	return nil, v.ErrorVersion
}

func (v *Version26Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity == 0 &&
		base.RawComponent == ""
}

func parse26CRUD(op string, counters map[string]int, payload record.MsgPayload) (record.MsgCRUD, bool) {
	if payload == nil {
		return record.MsgCRUD{}, false
	}

	// query test.foo query: { query: { a: 1.0 }, $comment: "comment", orderby: { b: 1.0 } } planSummary: EOF ntoreturn:0 ntoskip:0 nscanned:0 nscannedObjects:0 keyUpdates:0 numYields:0 locks(micros) r:99 nreturned:0 reslen:20 0ms
	// query test.foo query: { query: { a: 2.0, $comment: "comment" }, orderby: { b: 1.0 } } planSummary: EOF ntoreturn:0 ntoskip:0 nscanned:0 nscannedObjects:0 keyUpdates:0 numYields:0 locks(micros) r:41 nreturned:0 reslen:20 0ms
	// query test.foo query: { query: { a: 3.0 }, $comment: "comment" } planSummary: EOF ntoreturn:0 ntoskip:0 nscanned:0 nscannedObjects:0 keyUpdates:0 numYields:0 locks(micros) r:29 nreturned:0 reslen:20 0ms
	// query test.foo query: { a: 4.0, $comment: "comment" } planSummary: EOF ntoreturn:0 ntoskip:0 nscanned:0 nscannedObjects:0 keyUpdates:0 numYields:0 locks(micros) r:39 nreturned:0 reslen:20 0ms

	comment := ""
	query, ok := payload["query"].(map[string]interface{})
	if ok {
		if comment, ok = query["$comment"].(string); !ok {
			comment, _ = payload["$comment"].(string)
		}
	}

	switch op {
	case "query":
		c := record.MsgCRUD{
			Filter:  query,
			Comment: comment,
			N:       counters["nreturned"],
		}

		if query != nil {
			c.Sort, _ = query["orderby"].(map[string]interface{})
		}

		if c.Sort != nil || c.Comment != "" {
			if query, ok := payload["query"].(map[string]interface{}); ok {
				c.Filter = query
			}
		}

		return c, true

	case "update":
		return record.MsgCRUD{
			Filter:  query,
			Comment: comment,
			N:       counters["mModified"],
		}, true
	case "remove":
		return record.MsgCRUD{
			Filter:  query,
			Comment: comment,
			N:       counters["ndeleted"],
		}, true

	case "insert":
		return record.MsgCRUD{
			Filter:  query,
			Comment: comment,
			N:       counters["ninserted"],
		}, true

	default:
		return record.MsgCRUD{}, false
	}
}

func parse26Command(r util.RuneReader) (record.MsgCommandLegacy, error) {
	// command test.$cmd command: insert { insert: "foo", documents: [ { _id: ObjectId('59e3fdf50bae7edf962785a7'), a: 1.0 } ], ordered: true } keyUpdates:0 numYields:0 locks(micros) w:159 reslen:40 0ms
	var err error
	op := record.MakeMsgCommandLegacy()

	if opn, ok := r.SlurpWord(); ok {
		op.Command = opn
	} else {
		return record.MsgCommandLegacy{}, errors.New("operation not found")
	}

	if ns, ok := r.SlurpWord(); ok && strings.ContainsRune(ns, '.') {
		op.Namespace = ns
	} else {
		return record.MsgCommandLegacy{}, errors.New("no namespace found")
	}

	// Skip the "command:" portion, since it's irrelevant here.
	r.SkipWords(1)
	locks := false

	for {
		if param, ok := r.SlurpWord(); ok {
			if r.Expect('{') {
				if op.Payload[param], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return record.MsgCommandLegacy{}, err
				}
				op.Command = param
			} else if param == "locks(micros)" {
				locks = true
				continue
			} else if strings.HasPrefix(param, "ms") {
				if op.Duration, err = strconv.ParseInt(param[:len(param)-2], 10, 64); err != nil {
					return record.MsgCommandLegacy{}, err
				}
			} else if (locks && !parseIntegerKeyValue(param, op.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"})) ||
				!parseIntegerKeyValue(param, op.Counters, mongo.COUNTERS) {
				break
			}
		}
	}
	return op, nil
}

func parse26Operation(r util.RuneReader) (record.MsgOperationLegacy, error) {
	// getmore test.foo cursorid:30107363235 ntoreturn:3 keyUpdates:0 numYields:0 locks(micros) r:14 nreturned:3 reslen:119 0ms
	// insert test.foo query: { _id: ObjectId('5a331671de4f2a133f17884b'), a: 2.0 } ninserted:1 keyUpdates:0 numYields:0 locks(micros) w:10 0ms
	// remove test.foo query: { a: { $gte: 9.0 } } ndeleted:1 keyUpdates:0 numYields:0 locks(micros) w:63 0ms
	// update test.foo query: { a: { $gte: 8.0 } } update: { $set: { b: 1.0 } } nscanned:9 nscannedObjects:9 nMatched:1 nModified:1 keyUpdates:0 numYields:0 locks(micros) w:135 0ms
	var err error
	op := record.MakeMsgOperationLegacy()
	// Grab the operation name first.
	if opn, ok := r.SlurpWord(); ok {
		op.Operation = opn
	} else {
		return record.MsgOperationLegacy{}, ErrorVersionUnmatched{"unexpected operation"}
	}
	if ns, ok := r.SlurpWord(); ok && strings.ContainsRune(ns, '.') {
		op.Namespace = ns
	} else {
		r.RewindSlurpWord()
	}
	locks := false
	for param, ok := r.SlurpWord(); ok; param, ok = r.SlurpWord() {
		if strings.HasPrefix(param, "locks") {
			if param == "locks(micros)" {
				// Locks follow this param, so reset the target to the locks map.
				locks = true
				continue
			} else {
				// Wrong version.
				return record.MsgOperationLegacy{}, ErrorVersionUnmatched{}
			}
		} else if param == "planSummary:" {
			// Plan summaries require complicated and special code, so branch off and parse for plan summaries.
			if op.PlanSummary, err = parsePlanSummary(&r); err != nil {
				return record.MsgOperationLegacy{}, err
			}
			continue
		} else if length := len(param); length > 1 && util.ArrayBinarySearchString(param[:length-1], mongo.COMMANDS) {
			if r.EOL() {
				return record.MsgOperationLegacy{}, ErrorVersionUnmatched{"unexpected end of string"}
			} else if r.Expect('{') {
				// Parse JSON, found immediately after an operation.
				if op.Payload[param[:length-1]], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return record.MsgOperationLegacy{}, err
				}
			}
		} else if strings.ContainsRune(param, ':') {
			// A counter (in the form of key:value) needs to be applied to the correct target.
			if !locks || !parseIntegerKeyValue(param, op.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"}) {
				parseIntegerKeyValue(param, op.Counters, mongo.COUNTERS)
			}
		} else if strings.HasSuffix(param, "ms") {
			// Found a duration, which is also the last thing in the line.
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-3], 10, 64)
			break
		} else {
			// An unexpected value means that this parser either isn't the correct version or the line is invalid.
			return record.MsgOperationLegacy{}, ErrorVersionUnmatched{fmt.Sprintf("encountered unexpected value '%s'", param)}
		}
	}
	return op, nil
}
func (v *Version26Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 6, Binary: record.BinaryMongod}
}
