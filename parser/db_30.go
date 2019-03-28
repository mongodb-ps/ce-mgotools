package parser

import (
	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser/logger"
	"mgotools/record"
	"mgotools/util"
)

type Version30Parser struct {
	counters    map[string]string
	versionFlag bool
}

var errorVersion30Unmatched = internal.VersionUnmatched{Message: "version 3.0"}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version30Parser{

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
		return nil, errorVersion30Unmatched
	}
}

func (v *Version30Parser) Check(base record.Base) bool {
	return v.versionFlag &&
		base.RawSeverity != record.SeverityNone &&
		v.expectedComponents(base.RawComponent)
}

func (v *Version30Parser) command(r *util.RuneReader) (record.MsgCommand, error) {
	cmd, err := logger.CommandPreamble(r)
	if err != nil {
		return record.MsgCommand{}, err
	}

	err = logger.MidLoop(r, "locks:", &cmd.MsgBase, cmd.Counters, cmd.Payload, v.counters)
	if err != nil {
		if err == internal.CounterUnrecognized {
			v.versionFlag = false
			err = internal.VersionUnmatched{Message: "counter unrecognized"}
		}
		return record.MsgCommand{}, err
	}

	cmd.Locks, err = logger.Locks(r)
	if err != nil {
		return record.MsgCommand{}, err
	}

	cmd.Duration, err = logger.Duration(r)
	if err != nil {
		r.RewindSlurpWord()
		if r.ExpectString("protocol:") {
			v.versionFlag = false
		}
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

		return logger.CrudOrMessage(c, c.Command, c.Counters, c.Payload), nil

	} else {
		o, err := v.operation(r)
		if err != nil {
			return nil, err
		}

		return logger.CrudOrMessage(o, o.Operation, o.Counters, o.Payload), nil
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

func (v Version30Parser) operation(r *util.RuneReader) (record.MsgOperation, error) {
	op, err := logger.OperationPreamble(r)
	if err != nil {
		return op, err
	}

	err = logger.MidLoop(r, "locks:", &op.MsgBase, op.Counters, op.Payload, v.counters)
	if err != nil {
		return record.MsgOperation{}, err
	} else if !util.ArrayBinaryMatchString(op.Operation, mongo.OPERATIONS) {
		return record.MsgOperation{}, internal.OperationStructure
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

func (v *Version30Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 3, Minor: 0, Binary: record.BinaryMongod}
}
