package parser

import (
	"strconv"
	"strings"

	"mgotools/mongo"
	"mgotools/parser/errors"
	"mgotools/parser/logger"
	"mgotools/record"
	"mgotools/util"
)

type Version30Parser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version30Parser{VersionBaseParser: VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: errors.VersionUnmatched{"version 3.0"},
		}}
	})
}

func (v *Version30Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := *util.NewRuneReader(entry.RawMessage)

	// MDB 3.0 outputs commands and operations in a format almost identical to
	// MDB 2.6, which means we can use the legacy parser to handle the parsing.
	// The major difference is we have a component, so there's no need to
	// specially parse preamble (query, remove, command, etc).
	switch entry.RawComponent {
	case "COMMAND":
		c := r.PreviewWord(1)
		if c == "query" || c == "getmore" {
			return v.crud(false, &r)
		} else {
			return v.crud(true, &r)
		}

	case "WRITE":
		return v.crud(false, &r)

	default:
		return nil, errors.VersionMessageUnmatched
	}
}

func (v *Version30Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		v.expectedComponents(base.RawComponent)
}

func (Version30Parser) command(r *util.RuneReader) (record.MsgCommand, error) {
	var err error
	cmd := record.MakeMsgCommand()

	if c, n, o, err := logger.Preamble(r); err != nil {
		return record.MsgCommand{}, err
	} else if c != "command" {
		return record.MsgCommand{}, errors.CommandStructure
	} else {
		cmd.Command = o
		cmd.Namespace = n

		// Command is optional (but common), so if it doesn't exist then the
		// next thing on the line will be "planSummary:"
		if o != "command" {
			r.RewindSlurpWord()
		} else if op, ok := r.SlurpWord(); !ok {
			return record.MsgCommand{}, errors.CommandStructure
		} else {
			cmd.Command = op
			if cmd.Payload, err = mongo.ParseJsonRunes(r, false); err != nil {
				return record.MsgCommand{}, err
			}
		}

		cmd.Namespace = logger.NamespaceReplace(cmd.Command, cmd.Payload, cmd.Namespace)
	}

	for {
		param, ok := r.SlurpWord()
		if !ok {
			break
		}

		if ok, err := logger.SectionsStatic(param, &cmd.MsgBase, cmd.Payload, r); ok {
			continue
		} else if err != nil {
			return record.MsgCommand{}, nil
		} else if size := len(param); size > 6 && param[:6] == "locks:" {
			// Break the counter for loop and process lock information.
			r.RewindSlurpWord()
			r.Skip(6)
			break
		} else if strings.ContainsRune(param, ':') && !logger.IntegerKeyValue(param, cmd.Counters, mongo.COUNTERS) {
			return record.MsgCommand{}, errors.CounterUnrecognized
		}
	}

	if cmd.Locks, err = mongo.ParseJsonRunes(r, false); err != nil {
		return record.MsgCommand{}, err
	} else if word, ok := r.SlurpWord(); !ok || !strings.HasSuffix(word, "ms") {
		return record.MsgCommand{}, errors.CommandStructure
	} else if cmd.Duration, err = strconv.ParseInt(word[:len(word)-2], 10, 64); err != nil {
		return record.MsgCommand{}, err
	}

	return cmd, nil
}

func (v Version30Parser) crud(command bool, r *util.RuneReader) (record.Message, error) {
	if command {
		// This should be similar to handling in version 2.6.
		c, err := v.command(r)
		if err != nil {
			return nil, err
		}

		if crud, ok := logger.Crud(c.Command, c.Counters, c.Payload); ok {
			crud.Message = c
			return crud, nil
		}

		return c, err
	} else {
		o, err := v.operation(r)
		if err != nil {
			return nil, err
		}

		if crud, ok := logger.Crud(o.Operation, o.Counters, o.Payload); ok {
			crud.Message = o
			return crud, nil
		}

		return o, nil
	}
}

func (v *Version30Parser) expectedComponents(c string) bool {
	switch c {
	case "ACCESS",
		"ACCESSCONTROL",
		"BRIDGE",
		"COMMAND",
		"CONTROL",
		"DEFAULT",
		"GEO",
		"INDEX",
		"JOURNAL",
		"NETWORK",
		"QUERY",
		"REPL",
		"REPLICATION",
		"SHARDING",
		"STORAGE",
		"TOTAL",
		"WRITE",
		"-":
		return true
	default:
		return false
	}
}

func (Version30Parser) operation(r *util.RuneReader) (record.MsgOperation, error) {
	op := record.MakeMsgOperation()

	if c, n, _, err := logger.Preamble(r); err != nil {
		return record.MsgOperation{}, err
	} else {
		// Rewind the operation name so it can be parsed in the next section.
		r.RewindSlurpWord()

		op.Operation = c
		op.Namespace = n
	}

	for param, ok := r.SlurpWord(); ok; param, ok = r.SlurpWord() {
		if ok, err := logger.SectionsStatic(param, &op.MsgBase, op.Payload, r); err != nil {
			return record.MsgOperation{}, err
		} else if ok {
			continue
		}

		switch {
		case strings.HasPrefix(param, "locks:"):
			// The parameter is actually "locks:{", so rewind (which accounts
			// for spaces) and skip to the curly bracket.
			r.RewindSlurpWord()
			r.Skip(6)

			if locks, err := mongo.ParseJsonRunes(r, false); err != nil {
				return op, errors.OperationStructure
			} else {
				op.Locks = locks
			}

			// Immediately following the locks section is time.
			param, ok := r.SlurpWord()
			if !ok {
				continue
			}

			if !strings.HasSuffix(param, "ms") {
				return record.MsgOperation{}, errors.OperationStructure
			}

			// Found a duration, which is also the last thing in the line.
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-2], 10, 64)
			return op, nil

		case strings.ContainsRune(param, ':'):
			// A counter (in the form of key:value) needs to be applied to the correct target.
			if !logger.IntegerKeyValue(param, op.Counters, mongo.COUNTERS) {
				return record.MsgOperation{}, errors.CounterUnrecognized
			}

		default:
			if length := len(param); length > 1 && util.ArrayBinarySearchString(param[:length-1], mongo.OPERATIONS) {
				if r.EOL() {
					return record.MsgOperation{}, errors.OperationStructure
				}

				// Parse JSON, found immediately after an operation.
				var err error
				if op.Payload[param[:length-1]], err = mongo.ParseJsonRunes(r, false); err != nil {
					return record.MsgOperation{}, err
				}
			} else {
				// An unexpected value means that this parser either isn't the correct version or the line is invalid.
				return record.MsgOperation{}, errors.OperationStructure
			}
		}
	}

	return record.MsgOperation{}, errors.VersionUnmatched{}
}

func (v *Version30Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 0, Binary: record.BinaryMongod}
}
