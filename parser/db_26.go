// mongo/src/mongo/db/client.cpp

package parser

import (
	"fmt"
	"strconv"
	"strings"

	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version26Parser struct {
	crud     *executor.Executor
	contexts *executor.Executor
}

var errorVersion26Unmatched = internal.VersionUnmatched{Message: "version 2.6"}

func init() {
	version.Factory.Register(func() version.Parser {
		v := &Version26Parser{
			crud:     &executor.Executor{},
			contexts: &executor.Executor{},
		}

		messages := v.crud
		messages.RegisterForReader("command", v.commandCrud)

		messages.RegisterForReader("query", v.operationCrud)
		messages.RegisterForReader("getmore", v.operationCrud)
		messages.RegisterForReader("geonear", v.operationCrud)
		messages.RegisterForReader("insert", v.operationCrud)
		messages.RegisterForReader("update", v.operationCrud)
		messages.RegisterForReader("remove", v.operationCrud)

		context := v.contexts

		// initandlisten
		context.RegisterForReader("build info", mongodBuildInfo)
		context.RegisterForReader("db version", mongodDbVersion)
		context.RegisterForReader("journal dir=", mongodJournal)
		context.RegisterForReader("options", mongodOptions)

		context.RegisterForEntry("MongoDB starting", mongodStartupInfo)

		// signalProcessingThread
		context.RegisterForReader("dbexit", mongodParseShutdown)

		// connection related
		context.RegisterForReader("connection accepted", commonParseConnectionAccepted)
		context.RegisterForReader("waiting for connections", commonParseWaitingForConnections)
		context.RegisterForReader("successfully authenticated as principal", commonParseAuthenticatedPrincipal)

		context.RegisterForEntry("end connection", commonParseConnectionEnded)

		return v
	})
}

func (v *Version26Parser) NewLogMessage(entry record.Entry) (message.Message, error) {
	// Retrieve a value-based RuneReader because most functions don't take a
	// reference. This makes sense here because the RuneReader should be "reset"
	// on failure to parse. What better way to reset a RuneReader than to never
	// modify it in the first place, right?
	r := *internal.NewRuneReader(entry.RawMessage)
	switch {

	case entry.Connection > 0:
		msg, err := v.crud.Run(entry, &r, errorVersion26Unmatched)

		if err != errorVersion26Unmatched {
			return msg, err
		}

		// Check for network status changes, which have a context that
		// matches operations and commands.
		fallthrough

	default:
		msg, err := v.contexts.Run(entry, &r, errorVersion26Unmatched)

		if err != errorVersion26Unmatched {
			return msg, err
		} else {
			return nil, errorVersion26Unmatched
		}

	}
}

func (v Version26Parser) commandCrud(r *internal.RuneReader) (message.Message, error) {
	c, err := v.command(r)
	if err != nil {
		return c, err
	}

	return CrudOrMessage(c, c.Command, c.Counters, c.Payload), nil
}

func (Version26Parser) command(r *internal.RuneReader) (message.CommandLegacy, error) {
	var err error
	cmd := message.MakeCommandLegacy()

	if c, n, o, err := Preamble(r); err != nil {
		return message.CommandLegacy{}, err
	} else if c != "command" || o != "command" {
		return message.CommandLegacy{}, internal.CommandStructure
	} else {
		cmd.Command = c
		cmd.Namespace = n

		if word, ok := r.SlurpWord(); !ok {
			return message.CommandLegacy{}, internal.CommandStructure
		} else if cmd.Payload, err = Payload(r); err != nil {
			return message.CommandLegacy{}, err
		} else {
			cmd.Command = word
		}
	}

	cmd.Namespace = NamespaceReplace(cmd.Command, cmd.Payload, cmd.Namespace)
	counters := false
	for {
		param, ok := r.SlurpWord()
		if !ok {
			break
		}

		if !counters {
			if ok, err := StringSections(param, &cmd.BaseCommand, cmd.Payload, r); err != nil {
				return message.CommandLegacy{}, nil
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
				return message.CommandLegacy{}, err
			}
			break
		} else if strings.ContainsRune(param, ':') {
			if !IntegerKeyValue(param, cmd.Counters, record.COUNTERS) &&
				!IntegerKeyValue(param, cmd.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"}) {
				return message.CommandLegacy{}, internal.CounterUnrecognized
			}
		}
	}

	return cmd, nil
}

func (Version26Parser) Check(base record.Base) bool {
	return base.Severity == record.SeverityNone &&
		base.Component == record.ComponentNone
}

func (Version26Parser) operation(r *internal.RuneReader) (message.OperationLegacy, error) {
	// getmore test.foo cursorid:30107363235 ntoreturn:3 keyUpdates:0 numYields:0 locks(micros) r:14 nreturned:3 reslen:119 0ms
	// insert test.foo query: { _id: ObjectId('5a331671de4f2a133f17884b'), a: 2.0 } ninserted:1 keyUpdates:0 numYields:0 locks(micros) w:10 0ms
	// remove test.foo query: { a: { $gte: 9.0 } } ndeleted:1 keyUpdates:0 numYields:0 locks(micros) w:63 0ms
	// update test.foo query: { a: { $gte: 8.0 } } update: { $set: { b: 1.0 } } nscanned:9 nscannedObjects:9 nMatched:1 nModified:1 keyUpdates:0 numYields:0 locks(micros) w:135 0ms
	op := message.MakeOperationLegacy()

	if c, n, _, err := Preamble(r); err != nil {
		return message.OperationLegacy{}, err
	} else {
		// Rewind to capture the "query" portion of the line (or counter if
		// query doesn't exist).
		r.RewindSlurpWord()

		op.Operation = c
		op.Namespace = n
	}

	for param, ok := r.SlurpWord(); ok; param, ok = r.SlurpWord() {
		if ok, err := StringSections(param, &op.BaseCommand, op.Payload, r); err != nil {
			return op, err
		} else if ok {
			continue
		}

		switch {
		case strings.HasPrefix(param, "locks"):
			if param != "locks(micros)" {
				// Wrong version.
				return message.OperationLegacy{}, internal.VersionUnmatched{}
			}
			continue

		case !strings.HasSuffix(param, ":") && strings.ContainsRune(param, ':'):
			// A counter (in the form of key:value) needs to be applied to the correct target.
			if !IntegerKeyValue(param, op.Locks, map[string]string{"r": "r", "R": "R", "w": "w", "W": "W"}) &&
				!IntegerKeyValue(param, op.Counters, record.COUNTERS) {
				return message.OperationLegacy{}, internal.CounterUnrecognized
			}

		case strings.HasSuffix(param, "ms"):
			// Found a duration, which is also the last thing in the line.
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-2], 10, 64)
			return op, nil

		default:
			if length := len(param); length > 1 && internal.ArrayBinaryMatchString(param[:length-1], record.OPERATIONS) {
				if r.EOL() {
					return op, internal.OperationStructure
				}

				// Parse JSON, found immediately after an operation.
				var err error
				if op.Payload[param[:length-1]], err = mongo.ParseJsonRunes(r, false); err != nil {
					return op, err
				}
			} else {
				// An unexpected value means that this parser either isn't the correct version or the line is invalid.
				return message.OperationLegacy{}, internal.VersionUnmatched{Message: fmt.Sprintf("encountered unexpected value '%s'", param)}
			}
		}
	}

	return op, internal.CommandStructure
}

func (v Version26Parser) operationCrud(r *internal.RuneReader) (message.Message, error) {
	m, err := v.operation(r)
	if err != nil {
		return m, err
	}

	if crud, ok := Crud(m.Operation, m.Counters, m.Payload); ok {
		if m.Operation == "query" {
			// Standardize operation with later versions.
			m.Operation = "find"
		}

		crud.Message = m
		return crud, nil
	}

	return m, nil
}

func (Version26Parser) Version() version.Definition {
	return version.Definition{Major: 2, Minor: 6, Binary: record.BinaryMongod}
}
