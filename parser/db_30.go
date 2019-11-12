package parser

import (
	"mgotools/internal"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version30Parser struct {
	executor *executor.Executor

	counters    map[string]string
	versionFlag bool
}

var errorVersion30Unmatched = internal.VersionUnmatched{Message: "version 3.0"}

func init() {
	version.Factory.Register(func() version.Parser {
		ex := executor.New()

		// CONTROL component
		ex.RegisterForEntry("MongoDB starting", mongodStartupInfo)
		ex.RegisterForReader("db version", mongodDbVersion)
		ex.RegisterForReader("build info", mongodBuildInfo)
		ex.RegisterForReader("options", mongodOptions)
		ex.RegisterForReader("journal dir=", mongodJournal)
		ex.RegisterForReader("dbexit", mongodParseShutdown)

		// NETWORK component
		ex.RegisterForReader("waiting for connections", commonParseWaitingForConnections)
		ex.RegisterForReader("connection accepted", commonParseConnectionAccepted)
		ex.RegisterForEntry("end connection", commonParseConnectionEnded)

		return &Version30Parser{
			executor: ex,

			counters: map[string]string{
				"cursorid":        "cursorid",
				"ntoreturn":       "ntoreturn",
				"ntoskip":         "ntoskip",
				"exhaust":         "exhaust",
				"nscanned":        "keysExamined",
				"nscannedObjects": "docsExamined",
				"idhack":          "idhack",
				"scanAndOrder":    "scanAndOrder",
				"nmoved":          "nmoved",
				"nMatched":        "nmatched",
				"nModified":       "nmodified",
				"ninserted":       "ninserted",
				"ndeleted":        "ndeleted",
				"fastmod":         "fastmod",
				"fastmodinsert":   "fastmodinsert",
				"upsert":          "upsert",
				"keyUpdates":      "keyUpdates",
				"writeConflicts":  "writeConflicts",
				"nreturned":       "nreturned",
				"numYields":       "numYields",
				"reslen":          "reslend",
			},

			versionFlag: true,
		}
	})
}

func (v *Version30Parser) NewLogMessage(entry record.Entry) (message.Message, error) {
	r := internal.NewRuneReader(entry.RawMessage)

	// MDB 3.0 outputs commands and operations in a format almost identical to
	// MDB 2.6, which means we can use the legacy parser to handle the parsing.
	// The major difference is we have a component, so there's no need to
	// specially parse preamble (query, remove, command, etc).
	switch entry.Component {
	case record.ComponentCommand:
		c := r.PreviewWord(1)
		if c == "query" || c == "getmore" {
			return v.crud(false, r)
		} else {
			return v.crud(true, r)
		}

	case record.ComponentWrite:
		return v.crud(false, r)

	default:
		msg, err := v.executor.Run(entry, r, errorVersion30Unmatched)
		if err == nil {
			return msg, err
		}

		return nil, errorVersion30Unmatched
	}
}

func (v *Version30Parser) Check(base record.Base) bool {
	return v.versionFlag &&
		base.Severity != record.SeverityNone &&
		base.Severity >= record.SeverityD1 && base.Severity < record.SeverityD5 &&
		v.expectedComponents(base.Component)
}

func (v *Version30Parser) command(r *internal.RuneReader) (message.Command, error) {
	cmd, err := CommandPreamble(r)
	if err != nil {
		return message.Command{}, err
	}

	err = MidLoop(r, "locks:", &cmd.BaseCommand, cmd.Counters, cmd.Payload, v.counters)
	if err != nil {
		if err == internal.CounterUnrecognized {
			v.versionFlag = false
			err = internal.VersionUnmatched{Message: "counter unrecognized"}
		}
		return message.Command{}, err
	}

	cmd.Locks, err = Locks(r)
	if err != nil {
		return message.Command{}, err
	}

	cmd.Duration, err = Duration(r)
	if err != nil {
		r.RewindSlurpWord()
		if r.ExpectString("protocol:") {
			v.versionFlag = false
		}
		return message.Command{}, err
	}

	return cmd, nil
}

func (v Version30Parser) crud(command bool, r *internal.RuneReader) (message.Message, error) {
	if command {
		// This should be similar to handling in version 2.6.
		c, err := v.command(r)
		if err != nil {
			return nil, err
		}

		return CrudOrMessage(c, c.Command, c.Counters, c.Payload), nil

	} else {
		o, err := v.operation(r)
		if err != nil {
			return nil, err
		}

		return CrudOrMessage(o, o.Operation, o.Counters, o.Payload), nil
	}
}

func (v *Version30Parser) expectedComponents(c record.Component) bool {
	switch c {
	case record.ComponentAccess,
		record.ComponentAccessControl,
		record.ComponentBridge,
		record.ComponentCommand,
		record.ComponentControl,
		record.ComponentDefault,
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

func (v Version30Parser) operation(r *internal.RuneReader) (message.Operation, error) {
	op, err := OperationPreamble(r)
	if err != nil {
		return op, err
	}

	err = MidLoop(r, "locks:", &op.BaseCommand, op.Counters, op.Payload, v.counters)
	if err != nil {
		return message.Operation{}, err
	} else if !internal.ArrayBinaryMatchString(op.Operation, record.OPERATIONS) {
		return message.Operation{}, internal.OperationStructure
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

func (v *Version30Parser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 0, Binary: record.BinaryMongod}
}
