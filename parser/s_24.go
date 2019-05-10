package parser

import (
	"mgotools/internal"
	"mgotools/record"
	"mgotools/util"
)

type Version24SParser struct{}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version24SParser{}
	})
}

var errorVersion24SUnmatched = internal.VersionUnmatched{"mongos 2.4"}

func (v *Version24SParser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := util.NewRuneReader(entry.RawMessage)

	switch {
	case entry.Context == "mongosMain":
		if msg, err := S(entry).Control(r); err != nil {
			return msg, nil
		} else if r.ExpectString("connection accepted") {
			if ip, port, conn, ok := connectionInit(r); ok {
				return record.MsgConnection{
					Address: ip,
					Conn:    conn,
					Port:    port,
					Opened:  true,
				}, nil
			}
		}

	}
	return nil, errorVersion24SUnmatched
}

func (v *Version24SParser) Check(base record.Base) bool {
	return base.RawSeverity == record.SeverityNone &&
		base.RawComponent == ""
}

func (v *Version24SParser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 4, Binary: record.BinaryMongos}
}
