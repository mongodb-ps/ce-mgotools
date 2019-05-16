package parser

import (
	"fmt"

	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version32Parser struct {
	counters    map[string]string
	versionFlag bool
}

var errorVersion32Unmatched = internal.VersionUnmatched{Message: "version 3.2"}

func init() {
	version.Factory.Register(func() version.Parser {
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

func (v *Version32Parser) NewLogMessage(entry record.Entry) (message.Message, error) {
	r := *internal.NewRuneReader(entry.RawMessage)

	switch entry.RawComponent {
	case "COMMAND":
		// query, getmore, insert, update = COMMAND
		cmd, err := v.command(r)
		if err != nil {
			return cmd, err
		}

		return CrudOrMessage(cmd, cmd.Command, cmd.Counters, cmd.Payload), nil

	case "WRITE":
		// insert, remove, update = WRITE
		op, err := v.operation(r)
		if err != nil {
			return op, err
		}

		return CrudOrMessage(op, op.Operation, op.Counters, op.Payload), nil

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

func (v Version32Parser) command(reader internal.RuneReader) (message.Command, error) {
	r := &reader

	cmd, err := CommandPreamble(r)
	if err != nil {
		return message.Command{}, err
	} else if cmd.Agent != "" {
		// version 3.2 does not provide an agent string.
		v.versionFlag = false
		return message.Command{}, errorVersion32Unmatched
	}

	err = MidLoop(r, "locks:", &cmd.BaseCommand, cmd.Counters, cmd.Payload, v.counters)
	if err != nil {
		v.versionFlag, err = CheckCounterVersionError(err, errorVersion32Unmatched)
		return message.Command{}, err
	}

	cmd.Locks, err = Locks(r)
	if err != nil {
		return message.Command{}, err
	}

	cmd.Protocol, err = Protocol(r)
	if err != nil {
		return message.Command{}, err
	} else if cmd.Protocol != "op_query" && cmd.Protocol != "op_command" {
		v.versionFlag = false
		return message.Command{}, internal.VersionUnmatched{Message: fmt.Sprintf("unexpected protocol %s", cmd.Protocol)}
	}

	cmd.Duration, err = Duration(r)
	if err != nil {
		return message.Command{}, err
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

func (v *Version32Parser) operation(reader internal.RuneReader) (message.Operation, error) {
	r := &reader

	op, err := OperationPreamble(r)
	if err != nil {
		return op, err
	}

	err = MidLoop(r, "locks:", &op.BaseCommand, op.Counters, op.Payload, v.counters)
	if err != nil {
		v.versionFlag, err = CheckCounterVersionError(err, errorVersion32Unmatched)
		return message.Operation{}, err
	}

	op.Locks, err = Locks(r)
	if err != nil {
		return message.Operation{}, err
	}

	op.Duration, err = Duration(r)
	if err != nil {
		return message.Operation{}, err
	}

	return op, nil
}

func (v *Version32Parser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 2, Binary: record.BinaryMongod}
}
