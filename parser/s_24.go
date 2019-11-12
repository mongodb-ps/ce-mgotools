package parser

import (
	"mgotools/internal"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version24SParser struct {
	executor.Executor
}

func init() {
	parser := Version24SParser{}

	version.Factory.Register(func() version.Parser {
		return &parser
	})

	// Control (mongosMain)
	parser.RegisterForReader("build info", commonParseBuildInfo)
	parser.RegisterForReader("options:", mongosParseStartupOptions)
	parser.RegisterForReader("MongoS version", mongosParseVersion)

	// Network
	parser.RegisterForReader("connection accepted", commonParseConnectionAccepted)
	parser.RegisterForReader("waiting for connections", commonParseWaitingForConnections)
	parser.RegisterForEntry("end connection", commonParseConnectionEnded)
}

var errorVersion24SUnmatched = internal.VersionUnmatched{"mongos 2.4"}

func (v *Version24SParser) NewLogMessage(entry record.Entry) (msg message.Message, err error) {
	return v.Run(entry, internal.NewRuneReader(entry.RawMessage), errorVersion24SUnmatched)
}

func (v *Version24SParser) Check(base record.Base) bool {
	return base.Severity == record.SeverityNone &&
		base.Component == record.ComponentNone
}

func (v *Version24SParser) Version() version.Definition {
	return version.Definition{Major: 2, Minor: 4, Binary: record.BinaryMongos}
}
