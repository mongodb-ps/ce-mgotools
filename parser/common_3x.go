package parser

import (
	"bytes"
	"fmt"
	"mgotools/util"
	"strconv"
	"strings"
)

func parse3XCommand(msg *util.RuneReader) (LogMessage, error) {
	var (
		op      = LogMsgOpCommand{Counters: make(map[string]int), Locks: make(map[string]interface{})}
		name    string
		section struct {
					Meta bytes.Buffer
					Cmd  interface{}
				}
	)
	util.Debug("parse 3x started ...")
	// <command> <namespace> <suboperation>: <section[:]> <pattern>[, <section[:]> <pattern>] <counters> locks:<locks> [protocol:<protocol>] [duration]
	// Check for the operation first.
	op.Operation, _ = msg.SlurpWord()
	if !util.ArrayBinarySearchString(op.Operation, util.OPERATIONS) {
		return nil, fmt.Errorf("unexpected operation %s", op.Operation)
	}
	// Then for the namespace.
	op.Namespace, _ = msg.SlurpWord()
	if !strings.ContainsRune(op.Namespace, '.') {
		return nil, fmt.Errorf("unexpected namespace %s", op.Namespace)
	}
	// Then for the sub-operation.
	op.SubOperation, _ = msg.SlurpWord()
	if op.SubOperation == "" || op.SubOperation[len(op.SubOperation)-1] != ':' {
		return nil, fmt.Errorf("unexpected sub-operation %s", op.SubOperation)
	}
	// Remove the colon, which always exists.
	op.SubOperation = op.SubOperation[:len(op.SubOperation)-2]

	// Parse the remaining sections in a generic pattern.
	for param, ok := msg.SlurpWord(); ok && param != ""; param, ok = msg.SlurpWord() {
		util.Debug("Param: %s", param)

		if length := len(param); length > 0 {
			if pos := strings.IndexRune(param, ':'); pos > 0 {
				if pos == length-1 {
					if name != "" {
						sectionLength := section.Meta.Len()
						if sectionLength == 0 && section.Cmd != nil {
							op.Command[name] = section.Cmd
						} else if sectionLength > 0 && section.Cmd == nil {
							op.Command[name] = section.Meta.String()
						} else if sectionLength > 0 && section.Cmd != nil {
							op.Command[section.Meta.String()] = section.Cmd
						} else {
							panic("unexpected empty meta/cmd pairing")
						}
					}
					name = param[:length-1]
				} else if strings.HasPrefix(param, "locks:") {
					util.Debug("found locks: %s", param)
					msg.RewindSlurpWord()
					msg.Skip(6)
					util.Debug("next word at %s", msg.CurrentWord())
					op.Locks, _ = util.ParseJsonRunes(msg, false)
				} else {
					if _, ok := util.COUNTERS[param[:pos-1]]; ok {
						if count, err := strconv.ParseInt(param[pos+1:], 10, 32); err == nil {
							op.Counters[param[pos+1:]] = int(count)
						}
					} else {
						panic("unexpected counter type found: " + param[:pos-1])
					}
				}
			} else if length > 2 && param[length-2:length-1] == "ms" {
				op.Duration, _ = strconv.ParseInt(param[0:length-3], 10, 32)
			}
		}
	}
	util.Debug("%+v", op)
	return op, nil
}

func parse3XBuildIndex(r *util.RuneReader) (LogMessage, error) {
	// build index on: database.collection properties: { v: 2, key: { key1: 1.0 }, name: "name_1", ns: "database.collection" }
	var (
		err error
		msg LogMsgOpIndex
	)
	msg.Operation = "build index"
	msg.Namespace, _ = r.SkipWords(3).SlurpWord()
	if r.NextRune() == '{' {
		if msg.Properties, err = util.ParseJsonRunes(r.SkipWords(1), false); err != nil {
			return msg, nil
		}
	}
	return nil, err
}
