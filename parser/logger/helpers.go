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

	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/record"
	"mgotools/util"
)

func CommandPreamble(r *util.RuneReader) (record.MsgCommand, error) {
	cmd := record.MakeMsgCommand()

	if c, n, o, err := Preamble(r); err != nil {
		return record.MsgCommand{}, err
	} else if c != "command" {
		return record.MsgCommand{}, internal.CommandStructure
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
			return record.MsgCommand{}, internal.CommandStructure
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
	if err == internal.CounterUnrecognized {
		return true, v
	}

	return false, err
}

func Crud(op string, counters map[string]int64, payload record.MsgPayload) (record.MsgCRUD, bool) {
	if payload == nil {
		return record.MsgCRUD{}, false
	}

	filter, ok := payload["query"].(map[string]interface{})
	if !ok {
		// Newer versions do not contain a string prefixed section after an
		// operation and therefore will not have a wrapper around the query
		// and update portions.
		filter, _ = payload["q"].(map[string]interface{})
	}

	changes, ok := payload["update"].(map[string]interface{})
	if !ok {
		// Similar to query above, newer versions do not contain a string
		// section before an update so a secondary check is necessary.
		changes, _ = payload["u"].(map[string]interface{})
	}

	cursorId, _ := counters["cursorid"]
	comment, _ := payload["$comment"].(string)
	if comment == "" {
		comment, _ = filter["$comment"].(string)
		delete(filter, "$comment")
	}

	if _, explain := filter["$explain"]; explain {
		delete(filter, "$explain")
	}

	delete(payload, "$maxScan")
	delete(payload, "$returnKey")
	delete(payload, "$showDiskLoc")
	delete(payload, "$snapshot")
	delete(payload, "$maxTimeMS")

	if len(filter) == 1 {
		// Dollar operators may have existed but don't anymore, so remove
		// the superfluous "query" layer.
		if _, ok := filter["query"]; ok {
			filter, ok = filter["query"].(map[string]interface{})
		}
	}

	switch util.StringToLower(op) {
	case "find":
		return find(comment, cursorId, counters, payload)

	case "query":
		return query(comment, cursorId, counters, filter)

	case "update":
		return update(comment, counters, filter, changes)

	case "remove":
		return remove(comment, counters, filter)

	case "insert":
		return insert(comment, counters)

	case "count":
		return count(filter, payload)

	case "findandmodify":
		return findAndModify(cursorId, counters, filter, payload)

	case "geonear":
		return geoNear(cursorId, filter, payload)

	case "getmore":
		return record.MsgCRUD{CursorId: cursorId}, true
	}

	return record.MsgCRUD{}, false
}

func cleanQueryWithoutSort(c *record.MsgCRUD, query map[string]interface{}) {
	c.Sort, _ = query["orderby"].(map[string]interface{})
	if c.Sort != nil || c.Comment != "" {
		if query, ok := query["query"].(map[string]interface{}); ok {
			c.Filter = query
		}
	}
}

func count(query map[string]interface{}, payload record.MsgPayload) (record.MsgCRUD, bool) {
	if query == nil {
		return record.MsgCRUD{}, false
	}

	fields, _ := payload["fields"].(map[string]interface{})
	return record.MsgCRUD{
		Filter:  query,
		Project: fields,
	}, true
}

// A simple function that reduces CRUD checks and returns to a one-liner.
func CrudOrMessage(obj record.Message, term string, counters map[string]int64, payload record.MsgPayload) record.Message {
	if crud, ok := Crud(term, counters, payload); ok {
		crud.Message = obj
		return crud
	}

	return obj
}

// Returns a duration given a RuneReader. Expects a time in the format
// of <int>ms.
func Duration(r *util.RuneReader) (int64, error) {
	if word, ok := r.SlurpWord(); !ok {
		return 0, internal.UnexpectedEOL
	} else if !strings.HasSuffix(word, "ms") {
		return 0, internal.MisplacedWordException
	} else if dur, err := strconv.ParseInt(word[:len(word)-2], 10, 64); err != nil {
		return 0, err
	} else if dur < 0 {
		return 0, nil
	} else {
		return dur, nil
	}
}

func insert(comment string, counters map[string]int64) (record.MsgCRUD, bool) {
	crud := record.MsgCRUD{
		Update:  nil,
		Comment: comment,
		N:       counters["ninserted"],
	}

	return crud, true
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

func find(comment string, cursorId int64, counters map[string]int64, payload map[string]interface{}) (record.MsgCRUD, bool) {
	filter, ok := payload["filter"].(map[string]interface{})
	if !ok {
		return record.MsgCRUD{}, false
	}

	c := record.MsgCRUD{
		Comment:  comment,
		CursorId: cursorId,
		Filter:   filter,
		N:        counters["nreturned"],
	}

	cleanQueryWithoutSort(&c, filter)
	return c, true
}

func findAndModify(cursorId int64, counters map[string]int64, query map[string]interface{}, payload record.MsgPayload) (record.MsgCRUD, bool) {
	fields, _ := payload["fields"].(map[string]interface{})
	sort, _ := payload["sort"].(map[string]interface{})
	update, _ := payload["update"].(map[string]interface{})

	return record.MsgCRUD{
		CursorId: cursorId,
		Filter:   query,
		N:        counters["nModified"],
		Project:  fields,
		Sort:     sort,
		Update:   update,
	}, true
}

func geoNear(cursorId int64, query map[string]interface{}, payload record.MsgPayload) (record.MsgCRUD, bool) {
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
}

func Locks(r *util.RuneReader) (map[string]interface{}, error) {
	if !r.ExpectString("locks:{") {
		return nil, internal.UnexpectedVersionFormat
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
			return internal.UnexpectedVersionFormat
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
			return internal.CounterUnrecognized
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

// Operations have a different syntax but output similar information. This
// method processes lines that are typically WRITE.
func OperationPreamble(r *util.RuneReader) (record.MsgOperation, error) {
	op := record.MakeMsgOperation()
	// Grab the operation and namespace. Ignore the third portion of the
	// preamble because the reader will be rewound.

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
		return record.MsgPayload{}, internal.MisplacedWordException
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
		return nil, internal.NoPlanSummaryFound
	}
	return out, nil
}

func Preamble(r *util.RuneReader) (cmd, ns, op string, err error) {
	if word, ok := r.SlurpWord(); !ok {
		err = internal.UnexpectedEOL
		return
	} else {
		cmd = word
	}

	if word, ok := r.SlurpWord(); ok {
		ns = word
	} else {
		r.RewindSlurpWord()
		err = internal.UnexpectedEOL
		return
	}

	if name, ok := r.SlurpWord(); !ok {
		err = internal.UnexpectedEOL
	} else if size := len(name); name[size-1] == ':' && size > 1 {
		op = name[:size-1]
	} else {
		op = name
	}

	return
}

func Protocol(r *util.RuneReader) (string, error) {
	if !r.ExpectString("protocol:") {
		return "", internal.VersionMessageUnmatched
	}

	word, _ := r.SlurpWord()
	if len(word) < 10 {
		return "", internal.UnexpectedEOL
	}
	return word[9:], nil
}

func query(comment string, cursorId int64, counters map[string]int64, query map[string]interface{}) (record.MsgCRUD, bool) {
	// Before all operations were translated to "commands" in the log.
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

	cleanQueryWithoutSort(&c, query)

	return c, true
}

func remove(comment string, counters map[string]int64, filter map[string]interface{}) (record.MsgCRUD, bool) {
	return record.MsgCRUD{
		Filter:  filter,
		Comment: comment,
		N:       counters["ndeleted"],
	}, true
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
			err = internal.CommandStructure
			return false, err
		} else {
			base.Exception = exception
		}
	}

	return ok, nil
}

func update(comment string, counters map[string]int64, filter map[string]interface{}, update map[string]interface{}) (record.MsgCRUD, bool) {
	if filter == nil && update == nil {
		return record.MsgCRUD{}, false
	}

	crud := record.MsgCRUD{
		Comment: comment,
		Filter:  filter,
		Update:  update,
		N:       counters["nModified"],
	}
	return crud, true
}
