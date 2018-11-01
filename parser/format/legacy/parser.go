package legacy

import (
	"fmt"
	"strconv"
	"strings"

	"mgotools/mongo"
	"mgotools/parser/errors"
	"mgotools/parser/format"
	"mgotools/record"
	"mgotools/util"
)

func Crud(op string, counters map[string]int, payload record.MsgPayload) (record.MsgCRUD, bool) {
	if payload == nil {
		return record.MsgCRUD{}, false
	}

	// query test.foo query: { query: { a: 1.0 }, $comment: "comment", orderby: { b: 1.0 } } planSummary: EOF ntoreturn:0 ntoskip:0 nscanned:0 nscannedObjects:0 keyUpdates:0 numYields:0 locks(micros) r:99 nreturned:0 reslen:20 0ms
	// query test.foo query: { query: { a: 2.0, $comment: "comment" }, orderby: { b: 1.0 } } planSummary: EOF ntoreturn:0 ntoskip:0 nscanned:0 nscannedObjects:0 keyUpdates:0 numYields:0 locks(micros) r:41 nreturned:0 reslen:20 0ms
	// query test.foo query: { query: { a: 3.0 }, $comment: "comment" } planSummary: EOF ntoreturn:0 ntoskip:0 nscanned:0 nscannedObjects:0 keyUpdates:0 numYields:0 locks(micros) r:29 nreturned:0 reslen:20 0ms
	// query test.foo query: { a: 4.0, $comment: "comment" } planSummary: EOF ntoreturn:0 ntoskip:0 nscanned:0 nscannedObjects:0 keyUpdates:0 numYields:0 locks(micros) r:39 nreturned:0 reslen:20 0ms

	comment := ""
	query, ok := payload["query"].(map[string]interface{})
	comment, _ = payload["$comment"].(string)

	if ok {
		if comment == "" {
			comment, _ = query["$comment"].(string)
			delete(query, "$comment")
		}

		if _, explain := query["$explain"]; explain {
			delete(query, "$explain")
		}
		if len(query) == 1 {
			query, ok = query["query"].(map[string]interface{})
		}
	}

	switch util.StringToLower(op) {
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
			if query, ok := query["query"].(map[string]interface{}); ok {
				c.Filter = query
			}
		}

		return c, true

	case "update":
		update, ok := payload["update"].(map[string]interface{})
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

		fields, _ := query["fields"].(map[string]interface{})
		return record.MsgCRUD{
			Filter:  query,
			Project: fields,
		}, true

	case "findandmodify":
		fields, _ := payload["fields"].(map[string]interface{})
		sort, _ := payload["sort"].(map[string]interface{})
		update, _ := payload["update"].(map[string]interface{})

		return record.MsgCRUD{
			Filter:  query,
			Project: fields,
			Sort:    sort,
			Update:  update,
		}, true

	case "geonear":
		if near, ok := payload["near"].(map[string]interface{}); ok {
			if _, ok := near["$near"]; !ok {
				if query == nil {
					query = make(map[string]interface{})
				}
				query["$near"] = near
			}
		}

		return record.MsgCRUD{
			Filter: query,
		}, true

	case "getmore":
		return record.MsgCRUD{}, true
	}

	return record.MsgCRUD{}, false
}

func exception(r *util.RuneReader) (string, bool) {
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

func Command(r util.RuneReader) (record.MsgCommandLegacy, error) {
	// command test.$cmd command: insert { insert: "foo", documents: [ { _id: ObjectId('59e3fdf50bae7edf962785a7'), a: 1.0 } ], ordered: true } keyUpdates:0 numYields:0 locks(micros) w:159 reslen:40 0ms
	var err error
	op := record.MakeMsgCommandLegacy()

	if opn, ok := r.SlurpWord(); !ok || opn != "command" {
		return record.MsgCommandLegacy{}, errors.CommandNotFound
	}

	if ns, ok := r.SlurpWord(); ok && strings.ContainsRune(ns, '.') {
		op.Namespace = ns
	} else {
		return record.MsgCommandLegacy{}, errors.NoNamespaceFound
	}

	// Skip the "command:" portion, since it's irrelevant here.
	if cmd, _ := r.SlurpWord(); cmd != "command:" {
		return record.MsgCommandLegacy{}, errors.CommandStructure
	}

	if name, ok := r.SlurpWord(); ok {
		op.Command = name

		if r.Expect('{') {
			if op.Payload, err = mongo.ParseJsonRunes(&r, false); err != nil {
				return record.MsgCommandLegacy{}, err
			}
			if col, ok := op.Payload[op.Command].(string); ok && col != "" {
				op.Namespace = op.Namespace[:strings.IndexRune(op.Namespace, '.')+1] + col
			} else if col, ok := op.Payload[util.StringToLower(op.Command)].(string); ok && col != "" {
				op.Namespace = op.Namespace[:strings.IndexRune(op.Namespace, '.')+1] + col
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
			if op.PlanSummary, err = format.PlanSummary(&r); err != nil {
				return record.MsgCommandLegacy{}, err
			}

		case "update:":
			if op.Payload["update"], err = mongo.ParseJsonRunes(&r, false); err != nil {
				return record.MsgCommandLegacy{}, err
			}

		case "exception:":
			if op.Exception, ok = exception(&r); !ok {
				return record.MsgCommandLegacy{}, errors.CommandStructure
			}

		case "locks(micros)":
			continue

		default:
			if strings.HasSuffix(param, "ms") {
				if op.Duration, err = strconv.ParseInt(param[:len(param)-2], 10, 64); err != nil {
					return record.MsgCommandLegacy{}, err
				}
				return op, nil
			} else if !format.IntegerKeyValue(param, op.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"}) &&
				!format.IntegerKeyValue(param, op.Counters, mongo.COUNTERS) {
				return record.MsgCommandLegacy{}, errors.CounterUnrecognized
			}
		}
	}
	return op, errors.OperationStructure
}

func Operation(r util.RuneReader) (record.MsgOperationLegacy, error) {
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
		return record.MsgOperationLegacy{}, errors.VersionUnmatched{"unexpected operation"}
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
				return record.MsgOperationLegacy{}, errors.VersionUnmatched{}
			}

		case param == "planSummary:":
			// Plan summaries require complicated and special code, so branch off and parse for plan summaries.
			if op.PlanSummary, err = format.PlanSummary(&r); err != nil {
				return record.MsgOperationLegacy{}, err
			}
			continue

		case param == "exception:":
			if op.Exception, ok = exception(&r); !ok {
				return record.MsgOperationLegacy{}, errors.UnexpectedExceptionFormat
			}

		case !strings.HasSuffix(param, ":") && strings.ContainsRune(param, ':'):
			// A counter (in the form of key:value) needs to be applied to the correct target.
			if !format.IntegerKeyValue(param, op.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"}) &&
				!format.IntegerKeyValue(param, op.Counters, mongo.COUNTERS) {
				return record.MsgOperationLegacy{}, errors.CounterUnrecognized
			}

		case strings.HasSuffix(param, "ms"):
			// Found a duration, which is also the last thing in the line.
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-2], 10, 64)
			return op, nil

		default:
			if length := len(param); length > 1 && util.ArrayBinarySearchString(param[:length-1], mongo.OPERATIONS) {
				if r.EOL() {
					return record.MsgOperationLegacy{}, errors.VersionUnmatched{"unexpected end of string"}
				} else if r.Expect('{') {
					// Parse JSON, found immediately after an operation.
					if op.Payload[param[:length-1]], err = mongo.ParseJsonRunes(&r, false); err != nil {
						return record.MsgOperationLegacy{}, err
					}
				}
			} else {
				// An unexpected value means that this parser either isn't the correct version or the line is invalid.
				return record.MsgOperationLegacy{}, errors.VersionUnmatched{fmt.Sprintf("encountered unexpected value '%s'", param)}
			}
		}
	}

	return op, errors.CommandStructure
}
