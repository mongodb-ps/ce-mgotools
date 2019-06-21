package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version30SParser struct{}

func init() {
	version.Factory.Register(func() version.Parser {
		return &Version30SParser{}
	})
}

var errorVersion30SUnmatched = internal.VersionUnmatched{"mongos 3.0"}

func (v *Version30SParser) Check(base record.Base) bool {
	return base.Severity != record.SeverityNone &&
		base.Component != record.ComponentNone
}

func (v *Version30SParser) NewLogMessage(entry record.Entry) (msg message.Message, err error) {
	r := internal.NewRuneReader(entry.RawMessage)

	switch entry.Component {
	case record.ComponentSharding:
		if msg, err = S(entry).Control(*r); err == nil {
			return msg, nil
		}
	}

	return nil, errorVersion30SUnmatched
}
func (v *Version30SParser) Version() version.Definition {
	return version.Definition{Major: 3, Minor: 0, Binary: record.BinaryMongos}
}
