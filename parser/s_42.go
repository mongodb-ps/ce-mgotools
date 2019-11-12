package parser

import (
	"mgotools/internal"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

var errorVersion42SUnmatched = internal.VersionUnmatched{Message: "mongos 4.0"}

type Version42SParser struct{ executor.Executor }

func init() {
	parser := &Version42SParser{}

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

func (Version42SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.Component != record.ComponentNone
}

func (v *Version42SParser) NewLogMessage(entry record.Entry) (message.Message, error) {
	return v.Run(entry, internal.NewRuneReader(entry.RawMessage), errorVersion40SUnmatched)
}

func (Version42SParser) Version() version.Definition {
	return version.Definition{Major: 4, Minor: 2, Binary: record.BinaryMongos}
}
