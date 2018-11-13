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

func Crud(op string, counters map[string]int64, payload record.MsgPayload) (record.MsgCRUD, bool) {
	if payload == nil {
		return record.MsgCRUD{}, false
	}

	comment, _ := payload["$comment"].(string)
	cursorId, _ := counters["cursorid"]
	query, ok := payload["query"].(map[string]interface{})

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

func IntegerKeyValue(source string, target map[string]int64, limit map[string]string) bool {
	if key, num, ok := util.StringDoubleSplit(source, ':'); ok && num != "" {
		if _, ok := limit[key]; ok {
			if count, err := strconv.ParseInt(num, 10, 64); err == nil {
				target[key] = count
				return true
			} else {
				panic(err)
			}
		}
	}

	return false
}

func SectionsStatic(term string, base *record.MsgBase, payload record.MsgPayload, r *util.RuneReader) (ok bool, err error) {
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

/*
func SectionsVariable() {
	// Parse the remaining sections in a generic pattern.
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		length := util.StringLength(param)
		if pos := strings.IndexRune(param, ':'); pos > 0 {
			if pos == length-1 {
				if name != "" {
					sectionLength := section.Meta.Len()
					if sectionLength == 0 && section.Cmd != nil {
						op.Payload[name] = section.Cmd
					} else if sectionLength > 0 && section.Cmd == nil {
						if name == "appName" {
							op.Agent = section.Meta.String()
						} else {
							op.Payload[name] = section.Meta.String()
						}
					} else if sectionLength > 0 && section.Cmd != nil {
						op.Payload[section.Meta.String()] = section.Cmd
					} else {
						panic("unexpected empty meta/cmd pairing")
					}
				}
				name = param[:length-1]
			} else if strings.HasPrefix(param, "locks:") {
				r.RewindSlurpWord()
				r.Skip(6)
				op.Locks, err = mongo.ParseJsonRunes(&r, false)
			} else if strings.HasPrefix(param, "protocol:") {
				op.Protocol = param[9:]
			} else {
				if _, ok := mongo.COUNTERS[param[:pos]]; ok {
					if count, err := strconv.ParseInt(param[pos+1:], 10, 32); err == nil {
						op.Counters[param[:pos]] = int(count)
					}
				} else {
					panic("unexpected counter type found: '" + param[:pos] + "'")
				}
			}
		} else if param[0] == '{' {
			r.RewindSlurpWord()
			if name != "" {
				if op.Payload[name], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return record.MsgCommand{}, err
				}
			} else if section.Meta.Len() > 0 {
				if op.Payload[section.Meta.String()], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return record.MsgCommand{}, err
				}
				section.Meta.Reset()
			} else if op.Command != "" {
				name = op.Command
				if op.Payload[name], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return record.MsgCommand{}, err
				} else {
					op.Command = ""
				}
			} else if _, ok := op.Payload[op.Command]; !ok {
				if op.Payload[op.Command], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return record.MsgCommand{}, err
				}
			} else {
				if op.Payload["unknown"], err = mongo.ParseJsonRunes(&r, false); err != nil {
					return record.MsgCommand{}, err
				}
			}
			name = ""
		} else if unicode.Is(unicode.Quotation_Mark, rune(param[0])) {
			pos := r.Pos()
			r.RewindSlurpWord()
			if s, err := r.QuotedString(); err == nil {
				section.Meta.WriteString(s)
			} else {
				section.Meta.WriteString(param)
				section.Meta.WriteRune(' ')
				r.Seek(pos, 0)
			}
		} else if length > 2 && param[length-2:] == "ms" {
			op.Duration, _ = strconv.ParseInt(param[:length-2], 10, 32)
		} else if util.ArrayBinarySearchString(param, mongo.OPERATIONS) {
			name = util.StringToLower(param)
			op.Command = name
		} else {
			if section.Meta.Len() > 0 {
				section.Meta.WriteRune(' ')
			}
			section.Meta.WriteString(param)
		}
	}
	if section.Meta.Len() > 0 {
		if t, ok := op.Payload[op.Command].(map[string]interface{}); ok {
			if _, ok := t[section.Meta.String()]; ok {
				op.Command = section.Meta.String()
			}
		}
	}
}
*/

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
		err = errors.CommandNotFound
		return
	} else {
		cmd = word
	}

	if word, ok := r.SlurpWord(); ok && strings.ContainsRune(word, '.') {
		ns = word
	} else {
		r.RewindSlurpWord()
		err = errors.NoNamespaceFound
		return
	}

	if name, ok := r.SlurpWord(); !ok {
		err = errors.MisplacedWordException
	} else if size := len(name); name[size-1] == ':' && size > 1 {
		op = name[:size-1]
	}

	return
}

func Payload(r *util.RuneReader) (payload record.MsgPayload, err error) {
	if !r.ExpectRune('{') {
		return record.MsgPayload{}, errors.MisplacedWordException
	}

	payload, err = mongo.ParseJsonRunes(r, false)
	return
}
