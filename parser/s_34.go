package parser

import (
	"mgotools/internal"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version34SParser struct{ executor.Executor }

var errorVersion34SUnmatched = internal.VersionUnmatched{Message: "mongos 3.4"}

func init() {
	parser := &Version34SParser{}

	version.Factory.Register(func() version.Parser {
		return parser
	})

	parser.RegisterForReader("options:", mongosParseStartupOptions)
	parser.RegisterForReader("mongos version", mongosParseVersion)

	// Network
	parser.RegisterForReader("connection accepted", commonParseConnectionAccepted)
	parser.RegisterForReader("waiting for connections", commonParseWaitingForConnections)
	parser.RegisterForEntry("end connection", commonParseConnectionEnded)
}

func (v *Version34SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.Component != record.ComponentNone &&
		base.Severity >= record.SeverityD1 && base.Severity < record.SeverityD5
}

func (v *Version34SParser) NewLogMessage(entry record.Entry) (message.Message, error) {
	return v.Run(entry, internal.NewRuneReader(entry.RawMessage), errorVersion34SUnmatched)
}

func (v *Version34SParser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 4, Binary: record.BinaryMongos}
}
