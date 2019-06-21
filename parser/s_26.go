package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version26SParser struct{}

func init() {
	version.Factory.Register(func() version.Parser {
		return &Version26SParser{}
	})
}

var errorVersion26SUnmatched = internal.VersionUnmatched{Message: "mongos 2.6"}

func (v *Version26SParser) Check(base record.Base) bool {
	return base.Severity == record.SeverityNone &&
		base.Component == record.ComponentNone
}

func (v *Version26SParser) NewLogMessage(entry record.Entry) (msg message.Message, err error) {
	r := internal.NewRuneReader(entry.RawMessage)

	switch {
	case entry.Context == "mongosMain":
		if msg, err = S(entry).Control(*r); err == nil {
			return
		} else if msg, err = S(entry).Network(*r); err == nil {
			return
		}

	default:
		if msg, err = S(entry).Network(*r); err == nil {
			return msg, nil
		}
	}

	return nil, errorVersion26SUnmatched
}
func (v *Version26SParser) Version() version.Definition {
	return version.Definition{Major: 2, Minor: 6, Binary: record.BinaryMongos}
}
