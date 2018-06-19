package parser

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"mgotools/mongo"
	"mgotools/record"
	"mgotools/util"
)

func NormalizeQuery(cmd record.MsgOpCommandBase) (record.MsgQuery, bool) {
	convert := func(m interface{}) record.MsgQuery {
		if m != nil {
			if n, ok := m.(record.MsgQuery); ok {
				return n
			}
		}
		return record.MsgQuery{}
	}
	switch cmd.Command {
	case "count", "distinct":
		q, ok := cmd.Payload["query"]
		return convert(q), ok
	case "find", "getmore", "getMore":
		q, ok := cmd.Payload["filter"]
		return convert(q), ok
	case "geonear", "geoNear":
		q, ok := cmd.Payload["query"]
		c := convert(q)
		if ok && c != nil {
			c["near"], ok = cmd.Payload["near"]
		}
		return c, ok
	default:
		panic(fmt.Sprintf("unrecognzied query type during normalization: %s", cmd.Command))
	}
}

func parse3XCommand(r util.RuneReader, strict bool) (record.Message, error) {
	var (
		err     error
		ok      bool
		op      = record.MakeMsgOpCommand()
		name    string
		section struct {
			Meta bytes.Buffer
			Cmd  interface{}
		}
	)
	// <command> <namespace> <suboperation>: <section[:]> <pattern>[, <section[:]> <pattern>] <counters> locks:<locks> [protocol:<protocol>] [duration]
	// Check for the operation first.
	op.Operation, ok = r.SlurpWord()
	if !ok || (strict && !util.ArrayBinarySearchString(op.Operation, mongo.OPERATION_COMMANDS)) {
		return nil, VersionErrorUnmatched{"unexpected operation"}
	}

	// Then for the namespace.
	op.Namespace, ok = r.SlurpWord()
	if strict && (!ok || !strings.ContainsRune(op.Namespace, '.')) {
		return nil, VersionErrorUnmatched{"unexpected namespace"}
	} else if !strict && op.Namespace != "" && !strings.ContainsRune(op.Namespace, '.') {
		r.RewindSlurpWord()
		op.Command = op.Namespace
		op.Namespace = ""
	} else if strings.HasPrefix(r.PreviewWord(1), ":") {
		// Then for the sub-operation.
		op.Command, ok = r.SlurpWord()
		if !ok || op.Command == "" || op.Command[len(op.Command)-1] != ':' {
			return nil, VersionErrorUnmatched{"unexpected sub-operation"}
		}
		op.Command = op.Command[:len(op.Command)-1]
	}

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
					op.Errors = append(op.Errors, err)
				}
			} else if section.Meta.Len() > 0 {
				if op.Payload[section.Meta.String()], err = mongo.ParseJsonRunes(&r, false); err != nil {
					op.Errors = append(op.Errors, err)
				}
				section.Meta.Reset()
			} else if op.Command != "" {
				name = op.Command
				if op.Payload[name], err = mongo.ParseJsonRunes(&r, false); err != nil {
					op.Errors = append(op.Errors, err)
				} else {
					op.Command = ""
				}
			} else if _, ok := op.Payload[op.Operation]; !ok {
				if op.Payload[op.Operation], err = mongo.ParseJsonRunes(&r, false); err != nil {
					op.Errors = append(op.Errors, err)
				}
			} else {
				if op.Payload["unknown"], err = mongo.ParseJsonRunes(&r, false); err != nil {
					op.Errors = append(op.Errors, err)
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
		} else if util.ArrayBinarySearchString(param, mongo.OPERATION_COMMANDS) {
			name = util.StringToLower(param)
			op.Command = name
		} else {
			section.Meta.WriteString(param)
			section.Meta.WriteRune(' ')
		}
	}
	return op, nil
}

func parse3XBuildIndex(r util.RuneReader) (record.Message, error) {
	// build index on: database.collection properties: { v: 2, key: { key1: 1.0 }, name: "name_1", ns: "database.collection" }
	var (
		err error
		msg record.MsgOpIndex
	)
	msg.Operation = "build index"
	msg.Namespace, _ = r.SkipWords(3).SlurpWord()
	if r.ExpectString("properties:") {
		r.SkipWords(1)
	}
	if r.NextRune() == '{' {
		if msg.Properties, err = mongo.ParseJsonRunes(r.SkipWords(1), false); err != nil {
			return msg, nil
		}
	}
	return nil, VersionErrorUnmatched{"unmatched index build"}
}
