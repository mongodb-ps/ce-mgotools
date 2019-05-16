package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version24SParser struct{}

func init() {
	version.Factory.Register(func() version.Parser {
		return &Version24SParser{}
	})
}

var errorVersion24SUnmatched = internal.VersionUnmatched{"mongos 2.4"}

func (v *Version24SParser) NewLogMessage(entry record.Entry) (message.Message, error) {
	r := internal.NewRuneReader(entry.RawMessage)

	switch {
	case entry.Context == "mongosMain":
		if msg, err := S(entry).Control(*r); err == nil {
			return msg, nil
		} else if msg, err := S(entry).Network(*r); err == nil {
			return msg, nil
		}

	default:
		if msg, err := S(entry).Network(*r); err == nil {
			return msg, nil
		}
	}
	return nil, errorVersion24SUnmatched
}

func (v *Version24SParser) Check(base record.Base) bool {
	return base.Severity == record.SeverityNone &&
		base.RawComponent == "" &&
		base.CString
}

func (v *Version24SParser) Version() version.Definition {
	return version.Definition{Major: 2, Minor: 4, Binary: record.BinaryMongos}
}
