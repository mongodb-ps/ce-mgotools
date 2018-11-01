package modern

import (
	"bytes"
	"strconv"
	"strings"
	"unicode"

	"mgotools/mongo"
	"mgotools/parser/errors"
	"mgotools/parser/format"
	"mgotools/record"
	"mgotools/util"
)

func CommandStructure(r util.RuneReader, strict bool) (record.MsgCommand, error) {
	var (
		err     error
		op      = record.MakeMsgCommand()
		name    string
		section struct {
			Meta bytes.Buffer
			Cmd  interface{}
		}
	)

	// <command> <namespace> <operation>: <section[:]> <pattern>[, <section[:]> <pattern>] <counters> locks:<locks> [protocol:<protocol>] [duration]
	// Check for the operation first.
	if command, _ := r.SlurpWord(); command != "command" {
		return record.MsgCommand{}, errors.CommandStructure
	}

	op.Namespace, op.Command, err = structuredPreamble(&r, strict)
	if err != nil {
		return record.MsgCommand{}, err
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

	return op, nil
}

func messageFromComponent(entry record.Entry) (record.Message, error) {
	r := *entry.RuneReader
	switch entry.RawComponent {
	case "COMMAND":
		// query, getmore, insert, update = COMMAND
		if msg, err := CommandStructure(r, true); err == nil {
			return msg, err
		}

	case "WRITE":
		// insert, remove, update = WRITE
		if msg, err := OperationStructure(r, true); err == nil {
			return msg, err
		}

	case "INDEX":
		// TODO: Figure this out too.

	case "CONTROL":
		return format.Control(r, entry)

	case "NETWORK":
		if entry.RawContext == "command" {
			if msg, err := CommandStructure(r, false); err != nil {
				return msg, nil
			}
		}
		return format.Network(r, entry)

	case "STORAGE":
		return format.Storage(r, entry)
	}

	return nil, errors.ComponentUnmatched
}

func Message(entry record.Entry, err error) (record.Message, error) {
	if m, err := messageFromComponent(entry); err == nil {
		return m, nil
	}
	return nil, err
}

func OperationStructure(r util.RuneReader, strict bool) (record.MsgOperation, error) {
	panic("not implemented")
}

// Returns a namespace, command, and error given a reader. This matches the general
// preamble structure of a command and operation. The reader advances.
func structuredPreamble(r *util.RuneReader, strict bool) (string, string, error) {
	// Then for the namespace.
	ns, ok := r.SlurpWord()
	if strict && (!ok || !strings.ContainsRune(ns, '.')) {
		return "", "", errors.NoNamespaceFound
	} else if !strict && ns != "" && !strings.ContainsRune(ns, '.') {
		r.RewindSlurpWord()
		return "", ns, nil
	} else if strings.HasPrefix(r.PreviewWord(1), ":") {
		// Then for the sub-operation.
		cmd, ok := r.SlurpWord()
		size := len(cmd)
		if !ok || cmd == "" || cmd[size-1] != ':' {
			return "", "", errors.OperationStructure
		}

		return ns, cmd[:size-1], nil
	}

	return ns, "", nil
}
