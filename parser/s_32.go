package parser

import (
	"mgotools/internal"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version32SParser struct{ executor.Executor }

func init() {
	parser := Version32SParser{}
	version.Factory.Register(func() version.Parser {
		return &parser
	})

	parser.RegisterForReader("options:", mongosParseStartupOptions)
	parser.RegisterForReader("MongoS version", mongosParseVersion)

	// Network
	parser.RegisterForReader("connection accepted", commonParseConnectionAccepted)
	parser.RegisterForReader("waiting for connections", commonParseWaitingForConnections)
	parser.RegisterForEntry("end connection", commonParseConnectionEnded)
}

var errorVersion32SUnmatched = internal.VersionUnmatched{"mongos 3.2"}

func (v *Version32SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.Component != record.ComponentNone &&
		base.Severity >= record.SeverityD1 && base.Severity < record.SeverityD5
}

func (v *Version32SParser) NewLogMessage(entry record.Entry) (message.Message, error) {
	return v.Run(entry, internal.NewRuneReader(entry.RawMessage), errorVersion32SUnmatched)
}

func (v *Version32SParser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 2, Binary: record.BinaryMongos}
}
