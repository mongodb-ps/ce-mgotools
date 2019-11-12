package parser

import (
	"fmt"

	"mgotools/internal"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version32Parser struct {
	counters    map[string]string
	executor    *executor.Executor
	versionFlag bool
}

var errorVersion32Unmatched = internal.VersionUnmatched{Message: "version 3.2"}

func init() {
	version.Factory.Register(func() version.Parser {
		ex := executor.New()

		// CONTROL components
		ex.RegisterForReader("wiredtiger_open config", commonParseWiredtigerOpen) // 3.2+
		ex.RegisterForReader("db version", mongodDbVersion)
		ex.RegisterForReader("options", mongodOptions)
		ex.RegisterForReader("journal dir=", mongodJournal)
		ex.RegisterForReader("dbexit", mongodParseShutdown)

		// NETWORK component
		ex.RegisterForReader("waiting for connections", commonParseWaitingForConnections)
		ex.RegisterForReader("connection accepted", commonParseConnectionAccepted)
		ex.RegisterForEntry("end connection", commonParseConnectionEnded)

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

			executor:    ex,
			versionFlag: true,
		}
	})
}

func (v *Version32Parser) NewLogMessage(entry record.Entry) (message.Message, error) {
	r := *internal.NewRuneReader(entry.RawMessage)

	switch entry.Component {
	case record.ComponentCommand:
		// query, getmore, insert, update = COMMAND
		cmd, err := v.command(r)
		if err != nil {
			return cmd, err
		}

		return CrudOrMessage(cmd, cmd.Command, cmd.Counters, cmd.Payload), nil

	case record.ComponentWrite:
		// insert, remove, update = WRITE
		op, err := v.operation(r)
		if err != nil {
			return op, err
		}

		return CrudOrMessage(op, op.Operation, op.Counters, op.Payload), nil

	case record.ComponentNetwork:
		if entry.RawContext == "command" {
			if msg, err := v.command(r); err != nil {
				return msg, nil
			}
		}

		fallthrough

	default:
		return v.executor.Run(entry, &r, errorVersion32Unmatched)
	}
}

func (v *Version32Parser) Check(base record.Base) bool {
	return v.versionFlag &&
		base.Severity != record.SeverityNone &&
		base.Component != record.ComponentNone && v.expectedComponents(base.Component)
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

func (v *Version32Parser) expectedComponents(c record.Component) bool {
	switch c {
	case record.ComponentAccess,
		record.ComponentAccessControl,
		record.ComponentASIO,
		record.ComponentBridge,
		record.ComponentCommand,
		record.ComponentControl,
		record.ComponentDefault,
		record.ComponentExecutor,
		record.ComponentFTDC,
		record.ComponentGeo,
		record.ComponentIndex,
		record.ComponentJournal,
		record.ComponentNetwork,
		record.ComponentQuery,
		record.ComponentRepl,
		record.ComponentReplication,
		record.ComponentSharding,
		record.ComponentStorage,
		record.ComponentTotal,
		record.ComponentWrite,
		record.ComponentUnknown:
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
