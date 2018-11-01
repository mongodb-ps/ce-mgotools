// mongo/src/mongo/db/client.cpp

package parser

import (
	"mgotools/parser/errors"
	"mgotools/parser/format"
	"mgotools/parser/format/legacy"
	"mgotools/record"
	"mgotools/util"
)

type Version26Parser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version26Parser{VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_ISO8602_UTC, util.DATE_FORMAT_ISO8602_LOCAL}),
			ErrorVersion: errors.VersionUnmatched{Message: "version 2.6"},
		}}
	})
}

func (v *Version26Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := *util.NewRuneReader(entry.RawMessage)
	switch {
	case entry.Context == "initandlisten", entry.Context == "signalProcessingThread":
		// Check for control messages, which is almost everything in 2.6 that is logged at startup.
		if msg, err := format.Control(r, entry); err == nil {
			// Most startup messages are part of control.
			return msg, nil
		} else if msg, err := format.Network(r, entry); err == nil {
			// Alternatively, we care about basic network actions like new connections being established.
			return msg, nil
		}

	case v.currentOp(entry):
		switch {
		case r.ExpectString("command"):

			c, err := legacy.Command(r)
			if err != nil {
				return c, err
			}

			if crud, ok := legacy.Crud(c.Command, c.Counters, c.Payload); ok {
				crud.Message = c
				return crud, nil
			}

			return c, nil

		case r.ExpectString("query"),
			r.ExpectString("getmore"),
			r.ExpectString("geonear"),
			r.ExpectString("insert"),
			r.ExpectString("update"),
			r.ExpectString("remove"):

			m, err := legacy.Operation(r)
			if err != nil {
				return m, err
			}

			if crud, ok := legacy.Crud(m.Operation, m.Counters, m.Payload); ok {
				if m.Operation == "query" {
					// Standardize operation with later versions.
					m.Operation = "find"
				}

				crud.Message = m
				return crud, nil
			}

			return m, nil

		default:
			// Check for network status changes.
			if msg, err := format.Network(r, entry); err == nil {
				return msg, err
			}
		}
	}
	return nil, v.ErrorVersion
}

func (Version26Parser) currentOp(entry record.Entry) bool {
	// Current ops can be recorded by
	return entry.Connection > 0 ||
		entry.Context == "TTLMonitor"
}

func (Version26Parser) Check(base record.Base) bool {
	return !base.CString &&
		base.RawSeverity == 0 &&
		base.RawComponent == ""
}

func (Version26Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 6, Binary: record.BinaryMongod}
}
