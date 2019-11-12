package parser

import (
	"mgotools/internal"
	"mgotools/parser/executor"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version26SParser struct{ executor.Executor }

func init() {
	parser := Version24SParser{}
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

var errorVersion26SUnmatched = internal.VersionUnmatched{Message: "mongos 2.6"}

func (v *Version26SParser) Check(base record.Base) bool {
	return base.Severity == record.SeverityNone &&
		base.Component == record.ComponentNone
}

func (v *Version26SParser) NewLogMessage(entry record.Entry) (msg message.Message, err error) {
	return v.Run(entry, internal.NewRuneReader(entry.RawMessage), errorVersion26SUnmatched)
}
func (v *Version26SParser) Version() version.Definition {
	return version.Definition{Major: 2, Minor: 6, Binary: record.BinaryMongos}
}
