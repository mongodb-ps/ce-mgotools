package parser

import (
	"mgotools/internal"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version30SParser struct {
	executor.Executor
}

var errorVersion30SUnmatched = internal.VersionUnmatched{"mongos 3.0"}

func init() {
	parser := Version30SParser{
		executor.Executor{},
	}

	version.Factory.Register(func() version.Parser {
		return &parser
	})

	parser.RegisterForReader("build info", commonParseBuildInfo)
	parser.RegisterForReader("options:", mongosParseStartupOptions)
	parser.RegisterForReader("MongoS version", mongosParseVersion)

	// Network
	parser.RegisterForReader("connection accepted", commonParseConnectionAccepted)
	parser.RegisterForReader("waiting for connections", commonParseWaitingForConnections)
	parser.RegisterForEntry("end connection", commonParseConnectionEnded)
}

func (v *Version30SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.Component != record.ComponentNone
}

func (v *Version30SParser) NewLogMessage(entry record.Entry) (msg message.Message, err error) {
	return v.Run(entry, internal.NewRuneReader(entry.RawMessage), errorVersion30SUnmatched)
}
func (v *Version30SParser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 0, Binary: record.BinaryMongos}
}
