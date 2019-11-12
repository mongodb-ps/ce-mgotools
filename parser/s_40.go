package parser

import (
	"mgotools/internal"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

var errorVersion40SUnmatched = internal.VersionUnmatched{Message: "mongos 4.0"}

type Version40SParser struct{ executor.Executor }

func init() {
	parser := &Version40SParser{}

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

func (Version40SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.Component != record.ComponentNone
}

func (v *Version40SParser) NewLogMessage(entry record.Entry) (message.Message, error) {
	return v.Run(entry, internal.NewRuneReader(entry.RawMessage), errorVersion40SUnmatched)
}

func (Version40SParser) Version() version.Definition {
	return version.Definition{Major: 4, Minor: 0, Binary: record.BinaryMongos}
}
