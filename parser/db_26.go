// mongo/src/mongo/db/client.cpp

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

			c, err := v.parseCommand(r)
			if err != nil {
				return c, err
			}

			if crud, ok := v.parseCrud(c.Command, c.Counters, c.Payload); ok {
				crud.Message = c
				return crud, nil
			}

			return c, nil

		case r.ExpectString("query"),
			r.ExpectString("getmore"),
			r.ExpectString("geonear"),
			r.ExpectString("insert"),
			r.ExpectString("update"),
			r.ExpectString("remove"):

			m, err := v.parseOperation(r)
			if err != nil {
				return m, err
			}

			if crud, ok := v.parseCrud(m.Operation, m.Counters, m.Payload); ok {
				crud.Message = m
				return crud, nil
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

func (Version26Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity == 0 &&
		base.RawComponent == ""
}

func (Version26Parser) parseCrud(op string, counters map[string]int, payload record.MsgPayload) (record.MsgCRUD, bool) {
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
	case "find":
	case "query":
		c := record.MsgCRUD{
			Filter:  query,
			Comment: comment,
			N:       counters["nreturned"],
		}

		if query != nil {
			c.Sort, _ = query["orderby"].(record.MsgSort)
		}

		if c.Sort != nil || c.Comment != "" {
			if query, ok := payload["query"].(record.MsgFilter); ok {
				c.Filter = query
			}
		}

		return c, true

	case "update":
		update, ok := payload["update"].(record.MsgUpdate)
		if !ok || query == nil {
			break
		}

		return record.MsgCRUD{
			Filter:  query,
			Update:  update,
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
		if query == nil {
			break
		}

		id, _ := query["_id"]
		return record.MsgCRUD{
			Filter:  record.MsgFilter{"_id": id},
			Update:  query,
			Comment: comment,
			N:       counters["ninserted"],
		}, true

	case "count":
		if query == nil {
			return record.MsgCRUD{}, false
		}

		fields, _ := query["fields"].(record.MsgProject)
		return record.MsgCRUD{
			Filter:  query,
			Project: fields,
		}, true

	case "findandmodify":
		fields, _ := payload["fields"].(record.MsgProject)
		sort, _ := payload["sort"].(record.MsgSort)
		update, _ := payload["update"].(record.MsgUpdate)

		return record.MsgCRUD{
			Filter:  query,
			Project: fields,
			Sort:    sort,
			Update:  update,
		}, true

	case "geonear":
		if near, ok := payload["near"].(map[string]interface{}); ok {
			if _, ok := near["$near"]; !ok {
				query["$near"] = near
			}
		}

		return record.MsgCRUD{
			Filter: query,
		}, true
	}

	return record.MsgCRUD{}, false
}

func (Version26Parser) parseException(r *util.RuneReader) (string, bool) {
	start := r.Pos()
	if exception, ok := r.ScanForRune("numYields:"); !ok {
		r.Seek(start, 0)
	} else {
		// Rewind one since ScanForRune advances an extra character
		r.Prev()

		pos := strings.LastIndex(exception, " ")
		exception = strings.TrimRight(exception[:pos], " ")
		return exception, true
	}

	return "", false
}

func (v Version26Parser) parseCommand(r util.RuneReader) (record.MsgCommandLegacy, error) {
	// command test.$cmd command: insert { insert: "foo", documents: [ { _id: ObjectId('59e3fdf50bae7edf962785a7'), a: 1.0 } ], ordered: true } keyUpdates:0 numYields:0 locks(micros) w:159 reslen:40 0ms
	var err error
	op := record.MakeMsgCommandLegacy()

	if opn, ok := r.SlurpWord(); !ok || opn != "command" {
		return record.MsgCommandLegacy{}, ErrorCommandNotFound
	}

	if ns, ok := r.SlurpWord(); ok && strings.ContainsRune(ns, '.') {
		op.Namespace = ns[:strings.IndexRune(ns, '.')+1]
	} else {
		return record.MsgCommandLegacy{}, errors.New("no namespace found")
	}

	// Skip the "command:" portion, since it's irrelevant here.
	if cmd, _ := r.SlurpWord(); cmd != "command:" {
		return record.MsgCommandLegacy{}, ErrorCommandStructure
	}

	if name, ok := r.SlurpWord(); ok {
		op.Command = util.StringToLower(name)

		if r.Expect('{') {
			if op.Payload, err = mongo.ParseJsonRunes(&r, false); err != nil {
				return record.MsgCommandLegacy{}, err
			}
			if col, ok := op.Payload[op.Command].(string); ok && col != "" {
				op.Namespace = op.Namespace + col
			}
		}
	}

	for {
		param, ok := r.SlurpWord()
		if !ok {
			break
		}

		switch param {
		case "planSummary:":
			// Plan summaries require complicated and special code, so branch off and parse for plan summaries.
			if op.PlanSummary, err = parsePlanSummary(&r); err != nil {
				return record.MsgCommandLegacy{}, err
			}

		case "update:":
			if op.Payload["update"], err = mongo.ParseJsonRunes(&r, false); err != nil {
				return record.MsgCommandLegacy{}, err
			}

		case "exception:":
			if op.Exception, ok = v.parseException(&r); !ok {
				return record.MsgCommandLegacy{}, errors.New("error parsing exception")
			}

		case "locks(micros)":
			continue

		default:
			if strings.HasSuffix(param, "ms") {
				if op.Duration, err = strconv.ParseInt(param[:len(param)-2], 10, 64); err != nil {
					return record.MsgCommandLegacy{}, err
				}
				return op, nil
			} else if !parseIntegerKeyValue(param, op.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"}) &&
				!parseIntegerKeyValue(param, op.Counters, mongo.COUNTERS) {
				return record.MsgCommandLegacy{}, ErrorCounterUnrecognized
			}
		}
	}
	return op, ErrorOperationStructure
}

func (v Version26Parser) parseOperation(r util.RuneReader) (record.MsgOperationLegacy, error) {
	// getmore test.foo cursorid:30107363235 ntoreturn:3 keyUpdates:0 numYields:0 locks(micros) r:14 nreturned:3 reslen:119 0ms
	// insert test.foo query: { _id: ObjectId('5a331671de4f2a133f17884b'), a: 2.0 } ninserted:1 keyUpdates:0 numYields:0 locks(micros) w:10 0ms
	// remove test.foo query: { a: { $gte: 9.0 } } ndeleted:1 keyUpdates:0 numYields:0 locks(micros) w:63 0ms
	// update test.foo query: { a: { $gte: 8.0 } } update: { $set: { b: 1.0 } } nscanned:9 nscannedObjects:9 nMatched:1 nModified:1 keyUpdates:0 numYields:0 locks(micros) w:135 0ms
	var err error
	op := record.MakeMsgOperationLegacy()

	// Grab the operation name first.
	if opn, ok := r.SlurpWord(); ok {
		op.Operation = util.StringToLower(opn)
	} else {
		return record.MsgOperationLegacy{}, ErrorVersionUnmatched{"unexpected operation"}
	}
	if ns, ok := r.SlurpWord(); ok && strings.ContainsRune(ns, '.') {
		op.Namespace = ns
	} else {
		r.RewindSlurpWord()
	}

	for param, ok := r.SlurpWord(); ok; param, ok = r.SlurpWord() {
		switch {
		case strings.HasPrefix(param, "locks"):
			if param != "locks(micros)" {
				// Wrong version.
				return record.MsgOperationLegacy{}, ErrorVersionUnmatched{}
			}

		case param == "planSummary:":
			// Plan summaries require complicated and special code, so branch off and parse for plan summaries.
			if op.PlanSummary, err = parsePlanSummary(&r); err != nil {
				return record.MsgOperationLegacy{}, err
			}
			continue

		case param == "exception:":
			if op.Exception, ok = v.parseException(&r); !ok {
				return record.MsgOperationLegacy{}, errors.New("error parsing exception")
			}

		case strings.ContainsRune(param, ':'):
			// A counter (in the form of key:value) needs to be applied to the correct target.
			if !parseIntegerKeyValue(param, op.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"}) &&
				!parseIntegerKeyValue(param, op.Counters, mongo.COUNTERS) {
				return record.MsgOperationLegacy{}, ErrorCounterUnrecognized
			}

		case strings.HasSuffix(param, "ms"):
			// Found a duration, which is also the last thing in the line.
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-2], 10, 64)
			return op, nil

		default:
			if length := len(param); length > 1 && util.ArrayBinarySearchString(param[:length-1], mongo.OPERATIONS) {
				if r.EOL() {
					return record.MsgOperationLegacy{}, ErrorVersionUnmatched{"unexpected end of string"}
				} else if r.Expect('{') {
					// Parse JSON, found immediately after an operation.
					if op.Payload[param[:length-1]], err = mongo.ParseJsonRunes(&r, false); err != nil {
						return record.MsgOperationLegacy{}, err
					}
				}
			} else {
				// An unexpected value means that this parser either isn't the correct version or the line is invalid.
				return record.MsgOperationLegacy{}, ErrorVersionUnmatched{fmt.Sprintf("encountered unexpected value '%s'", param)}
			}
		}
	}

	return op, ErrorCommandStructure
}
func (Version26Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 6, Binary: record.BinaryMongod}
}
