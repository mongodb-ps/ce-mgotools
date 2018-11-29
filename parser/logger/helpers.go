// A logger helper functions exist because there are a lot of similarities
// between the different log formats. The similarities should have the same
// code path until the format diverges.
//
// Several different but similar methods are required to keep consistency
// between data types. Additionally, distinguishing between commands and
// operation means double the number of functions.
package logger

import (
	"strconv"
	"strings"

	"mgotools/mongo"
	"mgotools/parser/errors"
	"mgotools/record"
	"mgotools/util"
)

func CommandPreamble(r *util.RuneReader) (record.MsgCommand, error) {
	cmd := record.MakeMsgCommand()

	if c, n, o, err := Preamble(r); err != nil {
		return record.MsgCommand{}, err
	} else if c != "command" {
		return record.MsgCommand{}, errors.CommandStructure
	} else {
		if o == "appName" {
			cmd.Agent, err = r.QuotedString()
			if err != nil {
				return record.MsgCommand{}, err
			}

			o, _ = r.SlurpWord()
			if strings.HasSuffix(o, ":") {
				o = o[:len(o)-1]
			}
		}

		cmd.Command, cmd.Namespace = o, n

		// Command is optional (but common), so if it doesn't exist then the
		// next thing on the line will be "planSummary:" (or "appName:")
		if o != "command" {
			cmd.Command = ""
			r.RewindSlurpWord()
		} else if op, ok := r.SlurpWord(); !ok {
			return record.MsgCommand{}, errors.CommandStructure
		} else {
			// Cases where there are erratic commands will output JSON without
			// a command name. Record the command name if one exists, otherwise
			// backtrack and grab the payload.
			if op[0] != '{' {
				cmd.Command = op
			} else {
				r.RewindSlurpWord()
			}

			if cmd.Payload, err = mongo.ParseJsonRunes(r, false); err != nil {
				return record.MsgCommand{}, err
			}
		}

		cmd.Namespace = NamespaceReplace(cmd.Command, cmd.Payload, cmd.Namespace)
	}

	return cmd, nil
}

func CheckCounterVersionError(err error, v error) (bool, error) {
	if err == errors.CounterUnrecognized {
		return true, v
	}

	return false, err
}

func Crud(op string, counters map[string]int64, payload record.MsgPayload) (record.MsgCRUD, bool) {
	if payload == nil {
		return record.MsgCRUD{}, false
	}

	comment, _ := payload["$comment"].(string)
	cursorId, _ := counters["cursorid"]
	query, ok := payload["query"].(map[string]interface{})

	delete(payload, "$maxScan")
	delete(payload, "$returnKey")
	delete(payload, "$showDiskLoc")
	delete(payload, "$snapshot")
	delete(payload, "$maxTimeMS")

	if ok {
		if comment == "" {
			comment, _ = query["$comment"].(string)
			delete(query, "$comment")
		}

		if _, explain := query["$explain"]; explain {
			delete(query, "$explain")
		}
		if len(query) == 1 {
			// Dollar operators may have existed but don't anymore, so remove
			// the superfluous "query" layer.
			if _, ok := query["query"]; ok {
				query, ok = query["query"].(map[string]interface{})
			}
		}
	}

	switch util.StringToLower(op) {
	case "query":
		if query == nil {
			// "query" operations can exist without a filter that skip directly
			// to a plan summary. An empty filter should be returned.
			query = make(record.MsgFilter)
		}

		c := record.MsgCRUD{
			Comment:  comment,
			CursorId: cursorId,
			Filter:   query,
			N:        counters["nreturned"],
		}

		c.Sort, _ = query["orderby"].(map[string]interface{})
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
			Comment: comment,
			Filter:  query,
			Update:  update,
			N:       counters["nModified"],
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

		fields, _ := payload["fields"].(map[string]interface{})
		return record.MsgCRUD{
			Filter:  query,
			Project: fields,
		}, true

	case "findandmodify":
		fields, _ := payload["fields"].(map[string]interface{})
		sort, _ := payload["sort"].(map[string]interface{})
		update, _ := payload["update"].(map[string]interface{})

		return record.MsgCRUD{
			CursorId: cursorId,
			Filter:   query,
			Project:  fields,
			Sort:     sort,
			Update:   update,
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
			CursorId: cursorId,
			Filter:   query,
		}, true

	case "getmore":
		return record.MsgCRUD{CursorId: cursorId}, true
	}

	return record.MsgCRUD{}, false
}

// A simple function that reduces CRUD checks and returns to a one-liner.
func CrudOrMessage(obj record.Message, cmd string, counters map[string]int64, payload record.MsgPayload) record.Message {
	if crud, ok := Crud(cmd, counters, payload); ok {
		crud.Message = obj
		return crud
	}

	return obj
}

// Returns a duration given a RuneReader. Expects a time in the format
// of <int>ms.
func Duration(r *util.RuneReader) (int64, error) {
	if word, ok := r.SlurpWord(); !ok {
		return 0, errors.UnexpectedEOL
	} else if !strings.HasSuffix(word, "ms") {
		return 0, errors.MisplacedWordException
	} else if dur, err := strconv.ParseInt(word[:len(word)-2], 10, 64); err != nil {
		return 0, err
	} else if dur < 0 {
		return 0, nil
	} else {
		return dur, nil
	}
}

func IntegerKeyValue(source string, target map[string]int64, limit map[string]string) bool {
	if key, num, ok := util.StringDoubleSplit(source, ':'); ok && num != "" {
		if _, ok := limit[key]; ok {
			if num == "true" {
				target[key] = 1
				return true
			} else if num == "false" {
				target[key] = 0
				return true
			} else if count, err := strconv.ParseInt(num, 10, 64); err == nil {
				target[key] = count
				return true
			} else {
				panic(err)
			}
		}
	}

	return false
}

func Exception(r *util.RuneReader) (string, bool) {
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

func Locks(r *util.RuneReader) (map[string]interface{}, error) {
	if !r.ExpectString("locks:{") {
		return nil, errors.UnexpectedVersionFormat
	}

	r.Skip(6)
	lock, err := mongo.ParseJsonRunes(r, false)
	if err != nil {
		return nil, err
	}

	return lock, nil
}

// Loop through the middle portion containing counters until locks.
func MidLoop(r *util.RuneReader, stop string, base *record.MsgBase, counters map[string]int64, payload record.MsgPayload, check map[string]string) error {
	if check == nil {
		check = mongo.COUNTERS
	}

	for s := len(stop); ; {
		param, ok := r.SlurpWord()
		if !ok {
			return errors.UnexpectedVersionFormat
		}

		if ok, err := StringSections(param, base, payload, r); ok {
			continue
		} else if err != nil {
			return err
		}
		if l := len(param); l > 6 && param[:6] == "locks:" || l >= s && param[:s] == stop {
			r.RewindSlurpWord()
			break
		}
		if !IntegerKeyValue(param, counters, check) {
			return errors.CounterUnrecognized
		}
	}

	return nil
}

// Commands may overload the namespace to end in ".$cmd", which should
// be replaced by the collection name provided in the payload (if it exists).
func NamespaceReplace(c string, p record.MsgPayload, n string) string {
	if col, ok := p[c].(string); ok && col != "" {
		n = n[:strings.IndexRune(n, '.')+1] + col
	} else if col, ok := p[util.StringToLower(c)].(string); ok && col != "" {
		n = n[:strings.IndexRune(n, '.')+1] + col
	}
	return n
}

func OperationPreamble(r *util.RuneReader) (record.MsgOperation, error) {
	op := record.MakeMsgOperation()
	if c, n, _, err := Preamble(r); err != nil {
		return record.MsgOperation{}, err
	} else {
		// Rewind the operation name so it can be parsed in the next section.
		r.RewindSlurpWord()

		op.Operation = c
		op.Namespace = n
	}

	if r.ExpectString("appName:") {
		r.SkipWords(1)
		agent, err := r.QuotedString()
		if err == nil {
			op.Agent = agent
		} else {
			return record.MsgOperation{}, err
		}
	}

	return op, nil
}

func Payload(r *util.RuneReader) (payload record.MsgPayload, err error) {
	if !r.ExpectRune('{') {
		return record.MsgPayload{}, errors.MisplacedWordException
	}

	payload, err = mongo.ParseJsonRunes(r, false)
	return
}

func PlanSummary(r *util.RuneReader) ([]record.MsgPlanSummary, error) {
	var out []record.MsgPlanSummary
	for {
		if op, ok := r.SlurpWord(); !ok {
			// There are no words, so exit.
			break
		} else if r.NextRune() == '{' {
			if summary, err := mongo.ParseJsonRunes(r, false); err != nil {
				// The plan summary did not parse as valid JSON so exit.
				return nil, err
			} else {
				// The plan summary parsed as valid JSON, so record the operation and fall-through.
				out = append(out, record.MsgPlanSummary{op, summary})
			}
			if r.NextRune() != ',' {
				// There are no other plans so exit plan summary parsing.
				break
			} else {
				// There are more plans, so continue to run by repeating the for loop.
				r.Next()
				continue
			}
		} else if length := len(op); length > 2 && op[length-1] == ',' {
			// This is needed for repeated bare words (e.g. planSummary: COLLSCAN, COLLSCAN).
			out = append(out, record.MsgPlanSummary{op[:length-1], nil})
			continue
		} else {
			// Finally, the plan summary is boring and only includes a single word (e.g. COLLSCAN).
			out = append(out, record.MsgPlanSummary{op, nil})
			break
		}
	}
	if len(out) == 0 {
		// Return an error if no plans exist.
		return nil, errors.NoPlanSummaryFound
	}
	return out, nil
}

func Preamble(r *util.RuneReader) (cmd, ns, op string, err error) {
	if word, ok := r.SlurpWord(); !ok {
		err = errors.UnexpectedEOL
		return
	} else {
		cmd = word
	}

	if word, ok := r.SlurpWord(); ok {
		ns = word
	} else {
		r.RewindSlurpWord()
		err = errors.UnexpectedEOL
		return
	}

	if name, ok := r.SlurpWord(); !ok {
		err = errors.UnexpectedEOL
	} else if size := len(name); name[size-1] == ':' && size > 1 {
		op = name[:size-1]
	} else {
		op = name
	}

	return
}

func Protocol(r *util.RuneReader) (string, error) {
	if !r.ExpectString("protocol:") {
		return "", errors.VersionMessageUnmatched
	}

	word, _ := r.SlurpWord()
	_, prot, ok := util.StringDoubleSplit(word, ':')
	if !ok {
		return "", errors.UnexpectedEOL
	}

	return prot, nil
}

func StringSections(term string, base *record.MsgBase, payload record.MsgPayload, r *util.RuneReader) (ok bool, err error) {
	switch util.StringToLower(term) {
	case "query:", "update:":
		// Query and update are hard-coded into the logging code as specifically
		// placed values in the log line (if a document value exists).
		if payload[term[:len(term)-1]], err = mongo.ParseJsonRunes(r, false); err != nil {
			ok = false
			return
		}
		ok = true

	case "plansummary:":
		// Plan summaries require complicated and special code, so branch off
		// and parse for plan summaries. There may be times when multiple
		// plan summaries appear.
		if base.PlanSummary, err = PlanSummary(r); err != nil {
			ok = false
			return
		}

		ok = true

	case "exception:":
		ok = true
		if exception, ok := Exception(r); !ok {
			err = errors.CommandStructure
			return false, err
		} else {
			base.Exception = exception
		}
	}

	return ok, nil
}
