package parser

import (
	"bytes"
	"mgotools/mongo"
	"mgotools/util"
	"strconv"
	"strings"
	"unicode"
)

func parse3XCommand(r util.RuneReader, strict bool) (LogMessage, error) {
	var (
		err     error
		ok      bool
		op      = MakeLogMsgOpCommand()
		name    string
		section struct {
			Meta bytes.Buffer
			Cmd  interface{}
		}
	)
	// <command> <namespace> <suboperation>: <section[:]> <pattern>[, <section[:]> <pattern>] <counters> locks:<locks> [protocol:<protocol>] [duration]
	// Check for the operation first.
	op.Operation, ok = r.SlurpWord()
	if !ok || (strict && !util.ArrayBinarySearchString(op.Operation, mongo.OPERATIONS)) {
		return nil, LogVersionErrorUnmatched{"unexpected operation"}
	}
	// Then for the namespace.
	op.Namespace, ok = r.SlurpWord()
	if strict && (!ok || !strings.ContainsRune(op.Namespace, '.')) {
		return nil, LogVersionErrorUnmatched{"unexpected namespace"}
	} else if !strict && op.Namespace != "" && !strings.ContainsRune(op.Namespace, '.') {
		r.RewindSlurpWord()
		op.Name = op.Namespace
		op.Namespace = ""
	} else if strings.HasPrefix(r.PreviewWord(1), ":") {
		// Then for the sub-operation.
		op.Name, ok = r.SlurpWord()
		if !ok || op.Name == "" || op.Name[len(op.Name)-1] != ':' {
			return nil, LogVersionErrorUnmatched{"unexpected sub-operation"}
		}
		op.Name = op.Name[:len(op.Name)-1]
	}
	// Parse the remaining sections in a generic pattern.
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		length := util.StringLength(param)
		if pos := strings.IndexRune(param, ':'); pos > 0 {
			if pos == length-1 {
				if name != "" {
					sectionLength := section.Meta.Len()
					if sectionLength == 0 && section.Cmd != nil {
						op.Command[name] = section.Cmd
					} else if sectionLength > 0 && section.Cmd == nil {
						if name == "appName" {
							op.Agent = section.Meta.String()
						} else {
							op.Command[name] = section.Meta.String()
						}
					} else if sectionLength > 0 && section.Cmd != nil {
						op.Command[section.Meta.String()] = section.Cmd
					} else {
						panic("unexpected empty meta/cmd pairing")
					}
				}
				name = param[:length-1]
			} else if strings.HasPrefix(param, "locks:") {
				r.RewindSlurpWord()
				r.Skip(6)
				op.Locks, _ = util.ParseJsonRunes(&r, false)
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
				if op.Command[name], err = util.ParseJsonRunes(&r, false); err != nil {
					op.Errors = append(op.Errors, err)
				}
			} else if section.Meta.Len() > 0 {
				if op.Command[section.Meta.String()], err = util.ParseJsonRunes(&r, false); err != nil {
					op.Errors = append(op.Errors, err)
				}
				section.Meta.Reset()
			} else if op.Name != "" {
				name = op.Name
				if op.Command[name], err = util.ParseJsonRunes(&r, false); err != nil {
					op.Errors = append(op.Errors, err)
				} else {
					op.Name = ""
				}
			} else if _, ok := op.Command[op.Operation]; !ok {
				if op.Command[op.Operation], err = util.ParseJsonRunes(&r, false); err != nil {
					op.Errors = append(op.Errors, err)
				}
			} else {
				if op.Command["unknown"], err = util.ParseJsonRunes(&r, false); err != nil {
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
		} else if name == "command" {
			name = param
			op.Name = param
		} else {
			section.Meta.WriteString(param)
			section.Meta.WriteRune(' ')
		}
	}
	return op, nil
}

func parse3XBuildIndex(r util.RuneReader) (LogMessage, error) {
	// build index on: database.collection properties: { v: 2, key: { key1: 1.0 }, name: "name_1", ns: "database.collection" }
	var (
		err error
		msg LogMsgOpIndex
	)
	msg.Operation = "build index"
	msg.Namespace, _ = r.SkipWords(3).SlurpWord()
	if r.ExpectString("properties:") {
		r.SkipWords(1)
	}
	if r.NextRune() == '{' {
		if msg.Properties, err = util.ParseJsonRunes(r.SkipWords(1), false); err != nil {
			return msg, nil
		}
	}
	return nil, LogVersionErrorUnmatched{"unmatched index build"}
}
