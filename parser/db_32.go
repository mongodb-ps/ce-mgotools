package parser

import (
	"fmt"

	"mgotools/internal"
	"mgotools/parser/logger"
	"mgotools/record"
	"mgotools/util"
)

type Version32Parser struct {
	counters    map[string]string
	versionFlag bool
}

var errorVersion32Unmatched = internal.VersionUnmatched{Message: "version 3.2"}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version32Parser{
			counters: map[string]string{
				"cursorid":         "cursorid",
				"ntoreturn":        "ntoreturn",
				"ntoskip":          "notoskip",
				"exhaust":          "exhaust",
				"keysExamined":     "keysExamined",
				"docsExamined":     "docsExamined",
				"idhack":           "idhack",
				"hasSortStage":     "hasSortStage",
				"fromMultiPlanner": "fromMultiPlanner",
				"nmoved":           "nmoved",
				"nMatched":         "nmatched",
				"nModified":        "nmodified",
				"ninserted":        "ninserted",
				"ndeleted":         "ndeleted",
				"numYields":        "numYields",
				"nreturned":        "nreturned",
				"fastmod":          "fastmod",
				"fastmodinsert":    "fastmodinsert",
				"upsert":           "upsert",
				"cursorExhausted":  "cursorExhausted",
				"keyUpdates":       "keyUpdates",
				"writeConflicts":   "writeConflicts",
				"reslen":           "reslen",
			},

			versionFlag: true,
		}
	})
}

func (v *Version32Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := *util.NewRuneReader(entry.RawMessage)

	switch entry.RawComponent {
	case "COMMAND":
		// query, getmore, insert, update = COMMAND
		cmd, err := v.command(r)
		if err != nil {
			return cmd, err
		}

		return logger.CrudOrMessage(cmd, cmd.Command, cmd.Counters, cmd.Payload), nil

	case "WRITE":
		// insert, remove, update = WRITE
		op, err := v.operation(r)
		if err != nil {
			return op, err
		}

		return logger.CrudOrMessage(op, op.Operation, op.Counters, op.Payload), nil

	case "CONTROL":
		return D(entry).Control(r)

	case "NETWORK":
		if entry.RawContext == "command" {
			if msg, err := v.command(r); err != nil {
				return msg, nil
			}
		}
		return D(entry).Network(r)

	case "STORAGE":
		return D(entry).Storage(r)
	}

	return nil, errorVersion32Unmatched
}

func (v *Version32Parser) Check(base record.Base) bool {
	return v.versionFlag &&
		base.Severity != record.SeverityNone &&
		base.RawComponent != "" && v.expectedComponents(base.RawComponent)
}

func (v Version32Parser) command(reader util.RuneReader) (record.MsgCommand, error) {
	r := &reader

	cmd, err := logger.CommandPreamble(r)
	if err != nil {
		return record.MsgCommand{}, err
	} else if cmd.Agent != "" {
		// version 3.2 does not provide an agent string.
		v.versionFlag = false
		return record.MsgCommand{}, errorVersion32Unmatched
	}

	err = logger.MidLoop(r, "locks:", &cmd.MsgBase, cmd.Counters, cmd.Payload, v.counters)
	if err != nil {
		v.versionFlag, err = logger.CheckCounterVersionError(err, errorVersion32Unmatched)
		return record.MsgCommand{}, err
	}

	cmd.Locks, err = logger.Locks(r)
	if err != nil {
		return record.MsgCommand{}, err
	}

	cmd.Protocol, err = logger.Protocol(r)
	if err != nil {
		return record.MsgCommand{}, err
	} else if cmd.Protocol != "op_query" && cmd.Protocol != "op_command" {
		v.versionFlag = false
		return record.MsgCommand{}, internal.VersionUnmatched{Message: fmt.Sprintf("unexpected protocol %s", cmd.Protocol)}
	}

	cmd.Duration, err = logger.Duration(r)
	if err != nil {
		return record.MsgCommand{}, err
	}

	return cmd, nil
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

func (v *Version32Parser) operation(reader util.RuneReader) (record.MsgOperation, error) {
	r := &reader

	op, err := logger.OperationPreamble(r)
	if err != nil {
		return op, err
	}

	err = logger.MidLoop(r, "locks:", &op.MsgBase, op.Counters, op.Payload, v.counters)
	if err != nil {
		v.versionFlag, err = logger.CheckCounterVersionError(err, errorVersion32Unmatched)
		return record.MsgOperation{}, err
	}

	op.Locks, err = logger.Locks(r)
	if err != nil {
		return record.MsgOperation{}, err
	}

	op.Duration, err = logger.Duration(r)
	if err != nil {
		return record.MsgOperation{}, err
	}

	return op, nil
}

func (v *Version32Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 2, Binary: record.BinaryMongod}
}
