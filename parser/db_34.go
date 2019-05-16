package parser

import (
	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version34Parser struct {
	counters    map[string]string
	versionFlag bool
}

var errorVersion34Unmatched = internal.VersionUnmatched{Message: "version 3.2"}

func init() {
	version.Factory.Register(func() version.Parser {
		return &Version34Parser{
			counters: map[string]string{
				"cursorid":         "cursorid",
				"notoreturn":       "ntoreturn",
				"ntoskip":          "ntoskip",
				"exhaust":          "exhaust",
				"keysExamined":     "keysExamined",
				"docsExamined":     "docsExamined",
				"hasSortStage":     "hasSortStage",
				"fromMultiPlanner": "fromMultiPlanner",
				"replanned":        "replanned",
				"nMatched":         "nmatched",
				"nModified":        "nmodified",
				"ninserted":        "ninserted",
				"ndeleted":         "ndeleted",
				"fastmodinsert":    "fastmodinsert",
				"upsert":           "upsert",
				"cursorExhausted":  "cursorExhausted",
				"nmoved":           "nmoved",
				"keysInserted":     "keysInserted",
				"keysDeleted":      "keysDeleted",
				"writeConflicts":   "writeConflicts",
				"numYields":        "numYields",
				"reslen":           "reslen",
				"nreturned":        "nreturned",
			},

			versionFlag: true,
		}
	})
}

func (v *Version34Parser) Check(base record.Base) bool {
	return v.versionFlag &&
		base.Severity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version34Parser) command(reader internal.RuneReader) (message.Command, error) {
	r := &reader

	// Trivia: version 3.4 was the first to introduce app name metadata.
	cmd, err := CommandPreamble(r)
	if err != nil {
		return message.Command{}, err
	}

	if r.ExpectString("originatingCommand:") {
		r.SkipWords(1)
		cmd.Payload["originatingCommand"], err = mongo.ParseJsonRunes(r, false)

		if err != nil {
			return message.Command{}, err
		}
	}

	// Commands cannot have a "collation:" section, so this should be identical
	// to earlier versions (e.g. 3.2.x).
	err = MidLoop(r, "locks:", &cmd.BaseCommand, cmd.Counters, cmd.Payload, v.counters)
	if err != nil {
		v.versionFlag, err = CheckCounterVersionError(err, errorVersion34Unmatched)
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
		return message.Command{}, errorVersion34Unmatched
	}

	cmd.Duration, err = Duration(r)
	if err != nil {
		return message.Command{}, err
	}

	return cmd, nil
}

func (v *Version34Parser) expectedComponents(c string) bool {
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
		"TRACKING",
		"WRITE",
		"-":
		return true
	default:
		return false
	}
}

func (v *Version34Parser) NewLogMessage(entry record.Entry) (message.Message, error) {
	r := internal.NewRuneReader(entry.RawMessage)
	switch entry.RawComponent {
	case "COMMAND":
		cmd, err := v.command(*r)
		if err != nil {
			return nil, err
		}

		return CrudOrMessage(cmd, cmd.Command, cmd.Counters, cmd.Payload), nil

	case "WRITE":
		op, err := v.operation(*r)
		if err != nil {
			return nil, err
		}

		return CrudOrMessage(op, op.Operation, op.Counters, op.Payload), nil

	case "CONTROL":
		return D(entry).Control(*r)

	case "NETWORK":
		return D(entry).Network(*r)

	case "STORAGE":
		return D(entry).Storage(*r)
	}

	return nil, errorVersion34Unmatched
}

func (v *Version34Parser) operation(reader internal.RuneReader) (message.Operation, error) {
	r := &reader

	op, err := OperationPreamble(r)
	if err != nil {
		return message.Operation{}, err
	}

	if !internal.ArrayBinaryMatchString(op.Operation, []string{"command", "commandReply", "compressed", "getmore", "insert", "killcursors", "msg", "none", "query", "remove", "reply", "update"}) {
		v.versionFlag = false
		return message.Operation{}, errorVersion34Unmatched
	}

	for {
		// Collation appears in this version for the first time and doesn't
		// appear in any subsequent versions. It also only appears on WRITE
		// operations.
		err = MidLoop(r, "collation:", &op.BaseCommand, op.Counters, op.Payload, v.counters)
		if err != nil {
			v.versionFlag, err = CheckCounterVersionError(err, errorVersion34Unmatched)
			return message.Operation{}, err
		} else if r.ExpectString("collation:") {
			r.SkipWords(1)
			op.Payload["collation"], err = mongo.ParseJsonRunes(r, false)
			if err != nil {
				return message.Operation{}, err
			}
		} else {
			// This condition occurs after reaching "locks:".
			break
		}
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

func (v *Version34Parser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 4, Binary: record.BinaryMongod}
}
