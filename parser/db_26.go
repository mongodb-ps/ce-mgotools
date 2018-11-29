// mongo/src/mongo/db/client.cpp

package parser

import (
	"fmt"
	"strconv"
	"strings"

	"mgotools/mongo"
	"mgotools/parser/errors"
	"mgotools/parser/logger"
	"mgotools/record"
	"mgotools/util"
)

type Version26Parser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version26Parser{VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: errors.VersionUnmatched{Message: "version 2.6"},
		}}
	})
}

func (v *Version26Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	// Retrieve a value-based RuneReader because most functions don't take a
	// reference. This makes sense here because the RuneReader should be "reset"
	// on failure to parse. What better way to reset a RuneReader than to never
	// modify it in the first place, right?
	r := *util.NewRuneReader(entry.RawMessage)
	switch {
	case entry.Context == "initandlisten", entry.Context == "signalProcessingThread":
		// Check for control messages, which is almost everything in 2.6 that is logged at startup.
		if msg, err := logger.Control(r, entry); err == nil {
			// Most startup messages are part of control.
			return msg, nil
		} else if msg, err := logger.Network(r, entry); err == nil {
			// Alternatively, we care about basic network actions like new connections being established.
			return msg, nil
		}

	case v.currentOp(entry):
		switch {
		case r.ExpectString("command"):

			c, err := v.command(&r)
			if err != nil {
				return c, err
			}

			return logger.CrudOrMessage(c, c.Command, c.Counters, c.Payload), nil

		case r.ExpectString("query"),
			r.ExpectString("getmore"),
			r.ExpectString("geonear"),
			r.ExpectString("insert"),
			r.ExpectString("update"),
			r.ExpectString("remove"):

			m, err := v.operation(&r)
			if err != nil {
				return m, err
			}

			if crud, ok := logger.Crud(m.Operation, m.Counters, m.Payload); ok {
				if m.Operation == "query" {
					// Standardize operation with later versions.
					m.Operation = "find"
				}

				crud.Message = m
				return crud, nil
			}

			return m, nil

		default:
			// Check for network status changes.
			if msg, err := logger.Network(r, entry); err == nil {
				return msg, err
			}
		}
	}
	return nil, v.ErrorVersion
}

func (Version26Parser) command(r *util.RuneReader) (record.MsgCommandLegacy, error) {
	var err error
	cmd := record.MakeMsgCommandLegacy()

	if c, n, o, err := logger.Preamble(r); err != nil {
		return record.MsgCommandLegacy{}, err
	} else if c != "command" || o != "command" {
		return record.MsgCommandLegacy{}, errors.CommandStructure
	} else {
		cmd.Command = c
		cmd.Namespace = n

		if word, ok := r.SlurpWord(); !ok {
			return record.MsgCommandLegacy{}, errors.CommandStructure
		} else if cmd.Payload, err = logger.Payload(r); err != nil {
			return record.MsgCommandLegacy{}, err
		} else {
			cmd.Command = word
		}
	}

	cmd.Namespace = logger.NamespaceReplace(cmd.Command, cmd.Payload, cmd.Namespace)
	counters := false
	for {
		param, ok := r.SlurpWord()
		if !ok {
			break
		}

		if !counters {
			if ok, err := logger.StringSections(param, &cmd.MsgBase, cmd.Payload, r); err != nil {
				return record.MsgCommandLegacy{}, nil
			} else if ok {
				continue
			}
			if param == "locks(micros)" {
				counters = true
				continue
			}
		}
		if strings.HasSuffix(param, "ms") {
			if cmd.Duration, err = strconv.ParseInt(param[:len(param)-2], 10, 64); err != nil {
				return record.MsgCommandLegacy{}, err
			}
			break
		} else if strings.ContainsRune(param, ':') {
			if !logger.IntegerKeyValue(param, cmd.Counters, mongo.COUNTERS) &&
				!logger.IntegerKeyValue(param, cmd.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"}) {
				return record.MsgCommandLegacy{}, errors.CounterUnrecognized
			}
		}
	}

	return cmd, nil
}

func (Version26Parser) currentOp(entry record.Entry) bool {
	// Current ops can be recorded by
	return entry.Connection > 0 ||
		entry.Context == "TTLMonitor"
}

func (Version26Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity == 0 &&
		base.RawComponent == ""
}

func (Version26Parser) operation(r *util.RuneReader) (record.MsgOperationLegacy, error) {
	// getmore test.foo cursorid:30107363235 ntoreturn:3 keyUpdates:0 numYields:0 locks(micros) r:14 nreturned:3 reslen:119 0ms
	// insert test.foo query: { _id: ObjectId('5a331671de4f2a133f17884b'), a: 2.0 } ninserted:1 keyUpdates:0 numYields:0 locks(micros) w:10 0ms
	// remove test.foo query: { a: { $gte: 9.0 } } ndeleted:1 keyUpdates:0 numYields:0 locks(micros) w:63 0ms
	// update test.foo query: { a: { $gte: 8.0 } } update: { $set: { b: 1.0 } } nscanned:9 nscannedObjects:9 nMatched:1 nModified:1 keyUpdates:0 numYields:0 locks(micros) w:135 0ms
	op := record.MakeMsgOperationLegacy()

	if c, n, _, err := logger.Preamble(r); err != nil {
		return record.MsgOperationLegacy{}, err
	} else {
		// Rewind to capture the "query" portion of the line (or counter if
		// query doesn't exist).
		r.RewindSlurpWord()

		op.Operation = c
		op.Namespace = n
	}

	for param, ok := r.SlurpWord(); ok; param, ok = r.SlurpWord() {
		if ok, err := logger.StringSections(param, &op.MsgBase, op.Payload, r); err != nil {
			return op, err
		} else if ok {
			continue
		}

		switch {
		case strings.HasPrefix(param, "locks"):
			if param != "locks(micros)" {
				// Wrong version.
				return record.MsgOperationLegacy{}, errors.VersionUnmatched{}
			}
			continue

		case !strings.HasSuffix(param, ":") && strings.ContainsRune(param, ':'):
			// A counter (in the form of key:value) needs to be applied to the correct target.
			if !logger.IntegerKeyValue(param, op.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"}) &&
				!logger.IntegerKeyValue(param, op.Counters, mongo.COUNTERS) {
				return record.MsgOperationLegacy{}, errors.CounterUnrecognized
			}

		case strings.HasSuffix(param, "ms"):
			// Found a duration, which is also the last thing in the line.
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-2], 10, 64)
			return op, nil

		default:
			if length := len(param); length > 1 && util.ArrayBinarySearchString(param[:length-1], mongo.OPERATIONS) {
				if r.EOL() {
					return op, errors.OperationStructure
				}

				// Parse JSON, found immediately after an operation.
				var err error
				if op.Payload[param[:length-1]], err = mongo.ParseJsonRunes(r, false); err != nil {
					return op, err
				}
			} else {
				// An unexpected value means that this parser either isn't the correct version or the line is invalid.
				return record.MsgOperationLegacy{}, errors.VersionUnmatched{Message: fmt.Sprintf("encountered unexpected value '%s'", param)}
			}
		}
	}

	return op, errors.CommandStructure
}

func (Version26Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 6, Binary: record.BinaryMongod}
}
