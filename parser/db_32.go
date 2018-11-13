package parser

import (
	"strings"

	"mgotools/mongo"
	"mgotools/parser/errors"
	"mgotools/parser/logger"
	"mgotools/record"
	"mgotools/util"
)

type Version32Parser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version32Parser{VersionBaseParser: VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: errors.VersionUnmatched{Message: "version 3.2"},
		}}
	})
}

func (v *Version32Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != "" && v.expectedComponents(base.RawComponent)
}

func (v *Version32Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := util.NewRuneReader(entry.RawMessage)
	msg, err := v.command(r)
	return msg, err
}

func (Version32Parser) command(r *util.RuneReader) (record.MsgCommand, error) {
	cmd := record.MakeMsgCommand()

	// <command> <namespace> <operation>: <section[:]> <pattern>[, <section[:]> <pattern>] <counters> locks:<locks> [protocol:<protocol>] [duration]
	// Check for the operation first.
	if c, n, o, err := logger.Preamble(r); err != nil {
		return record.MsgCommand{}, err
	} else if c != "command" {
		return record.MsgCommand{}, errors.CommandStructure
	} else {
		cmd.Command = o
		cmd.Namespace = n

		if o != "command" {
			r.RewindSlurpWord()
		} else if word, ok := r.SlurpWord(); !ok {
			return record.MsgCommand{}, errors.CommandStructure
		} else {
			cmd.Command = word

			if cmd.Payload[cmd.Command], err = mongo.ParseJsonRunes(r, false); err != nil {
				return record.MsgCommand{}, errors.CommandStructure
			}
		}
	}

	for {
		param, ok := r.SlurpWord()
		if !ok {
			break
		}

		if _, err := logger.SectionsStatic(param, &cmd.MsgBase, cmd.Payload, r); err != nil {
			return record.MsgCommand{}, nil
		} else if strings.ContainsRune(param, ':') {
			if !logger.IntegerKeyValue(param, cmd.Counters, mongo.COUNTERS) {
				return record.MsgCommand{}, errors.CounterUnrecognized
			}
		}

		if strings.HasPrefix(param, "locks:") {
			r.RewindSlurpWord()
			r.Skip(6)
			break
		}
	}

	return cmd, nil
}

/*
func MessageFromComponent(entry record.Entry) (record.Message, error) {
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
		return logger.Control(r, entry)

	case "NETWORK":
		if entry.RawContext == "command" {
			if msg, err := CommandStructure(r, false); err != nil {
				return msg, nil
			}
		}
		return logger.Network(r, entry)

	case "STORAGE":
		return logger.Storage(r, entry)
	}

	return nil, errors.ComponentUnmatched
}

*/

func (v *Version32Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 2, Binary: record.BinaryMongod}
}

func (v *Version32Parser) expectedComponents(c string) bool {
	switch c {
	case "ACCESS",
		"ACCESSCONTROL",
		"ASIO",
		"BRIDGE",
		"COMMAND",
		"CONTROL",
		"DEFAULT",
		"EXECUTOR",
		"FTDC",
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
