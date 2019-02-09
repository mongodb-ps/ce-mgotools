package parser

import (
	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser/logger"
	"mgotools/record"
	"mgotools/util"
)

type Version34Parser struct {
	VersionBaseParser

	counters    map[string]string
	versionFlag bool
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version34Parser{
			VersionBaseParser: VersionBaseParser{
				ErrorVersion: internal.VersionUnmatched{Message: "version 3.4"},
			},

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
		base.RawSeverity != record.SeverityNone &&
		base.RawComponent != ""
}

func (v *Version34Parser) command(reader util.RuneReader) (record.MsgCommand, error) {
	r := &reader

	// Trivia: Version 3.4 was the first to introduce app name metadata.
	cmd, err := logger.CommandPreamble(r)
	if err != nil {
		return record.MsgCommand{}, err
	}

	if r.ExpectString("originatingCommand:") {
		r.SkipWords(1)
		cmd.Payload["originatingCommand"], err = mongo.ParseJsonRunes(r, false)

		if err != nil {
			return record.MsgCommand{}, err
		}
	}

	// Commands cannot have a "collation:" section, so this should be identical
	// to earlier versions (e.g. 3.2.x).
	err = logger.MidLoop(r, "locks:", &cmd.MsgBase, cmd.Counters, cmd.Payload, v.counters)
	if err != nil {
		v.versionFlag, err = logger.CheckCounterVersionError(err, v.ErrorVersion)
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
		return record.MsgCommand{}, v.ErrorVersion
	}

	cmd.Duration, err = logger.Duration(r)
	if err != nil {
		return record.MsgCommand{}, err
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

func (v *Version34Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := util.NewRuneReader(entry.RawMessage)
	switch entry.RawComponent {
	case "COMMAND":
		cmd, err := v.command(*r)
		if err != nil {
			return nil, err
		}

		return logger.CrudOrMessage(cmd, cmd.Command, cmd.Counters, cmd.Payload), nil

	case "WRITE":
		op, err := v.operation(*r)
		if err != nil {
			return nil, err
		}

		return logger.CrudOrMessage(op, op.Operation, op.Counters, op.Payload), nil

	case "CONTROL":
		return logger.Control(*r, entry)

	case "NETWORK":
		return logger.Network(*r, entry)

	case "STORAGE":
		return logger.Storage(*r, entry)
	}

	return nil, v.ErrorVersion
}

func (v *Version34Parser) operation(reader util.RuneReader) (record.MsgOperation, error) {
	r := &reader

	op, err := logger.OperationPreamble(r)
	if err != nil {
		return record.MsgOperation{}, err
	}

	if !util.ArrayBinaryMatchString(op.Operation, []string{"command", "commandReply", "compressed", "getmore", "insert", "killcursors", "msg", "none", "query", "remove", "reply", "update"}) {
		v.versionFlag = false
		return record.MsgOperation{}, v.ErrorVersion
	}

	for {
		// Collation appears in this version for the first time and doesn't
		// appear in any subsequent versions. It also only appears on WRITE
		// operations.
		err = logger.MidLoop(r, "collation:", &op.MsgBase, op.Counters, op.Payload, v.counters)
		if err != nil {
			v.versionFlag, err = logger.CheckCounterVersionError(err, v.ErrorVersion)
			return record.MsgOperation{}, err
		} else if r.ExpectString("collation:") {
			r.SkipWords(1)
			op.Payload["collation"], err = mongo.ParseJsonRunes(r, false)
			if err != nil {
				return record.MsgOperation{}, err
			}
		} else {
			// This condition occurs after reaching "locks:".
			break
		}
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

func (v *Version34Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 4, Binary: record.BinaryMongod}
}
