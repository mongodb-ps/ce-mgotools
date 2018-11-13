package parser

import (
	"strconv"
	"strings"

	"mgotools/mongo"
	"mgotools/parser/errors"
	"mgotools/parser/logger"
	"mgotools/record"
	"mgotools/util"
)

type Version24Parser struct {
	VersionBaseParser
}

func init() {
	VersionParserFactory.Register(func() VersionParser {
		return &Version24Parser{VersionBaseParser{
			DateParser:   util.NewDateParser([]string{util.DATE_FORMAT_CTIMENOMS, util.DATE_FORMAT_CTIME, util.DATE_FORMAT_CTIMEYEAR}),
			ErrorVersion: errors.VersionUnmatched{Message: "version 2.4"},
		}}
	})
}

func (v *Version24Parser) NewLogMessage(entry record.Entry) (record.Message, error) {
	r := *util.NewRuneReader(entry.RawMessage)
	switch {
	case entry.Context == "initandlisten", entry.Context == "signalProcessingThread":
		// Check for control messages, which is almost everything in 2.4 that is logged at startup.
		if msg, err := logger.Control(r, entry); err == nil {
			// Most startup messages are part of control.
			return msg, nil
		} else if msg, err := logger.Network(r, entry); err == nil {
			// Alternatively, we care about basic network actions like new connections being established.
			return msg, nil
		}

	case entry.Connection > 0:
		// Check for connection related messages, which is almost everything *not* related to startup messages.
		switch {
		case r.ExpectString("query"),
			r.ExpectString("update"),
			r.ExpectString("remove"):
			// Commands or queries!
			op, err := v.parse24WithPayload(r, false)
			if err != nil {
				return nil, err
			}

			if c, ok := logger.Crud(op.Operation, op.Counters, op.Payload); ok {
				c.Message = op
				return c, nil
			}

			return op, err

		case r.ExpectString("command"):
			// Commands in 2.4 don't include anything that should be converted
			// into operations (e.g. find, update, remove, etc).
			if op, err := v.parse24WithPayload(r, true); err != nil {
				return nil, err
			} else {
				cmd := record.MakeMsgCommandLegacy()
				cmd.Command = op.Operation
				cmd.Duration = op.Duration
				cmd.Namespace = logger.NamespaceReplace(op.Operation, op.Payload, op.Namespace)
				cmd.Locks = op.Locks
				cmd.Payload = op.Payload

				if c, ok := logger.Crud(op.Operation, op.Counters, op.Payload); ok {
					c.Message = cmd
					return c, nil
				}

				return cmd, err
			}

		case r.ExpectString("insert"):
			// Inserts!
			return v.parse24WithoutPayload(r)

		case r.ExpectString("getmore"):
			op, err := v.parse24WithoutPayload(r)
			if err != nil {
				return nil, err
			}

			if c, ok := logger.Crud(op.Operation, op.Counters, op.Payload); ok {
				c.Message = op
				return c, nil
			}

			return op, nil

		default:
			// Check for network connection changes.
			if msg, err := logger.Network(r, entry); err == nil {
				return msg, nil
			}
		}
	}
	return nil, v.ErrorVersion
}

func (v *Version24Parser) Check(base record.Base) bool {
	return base.RawComponent == "" &&
		base.RawSeverity == record.SeverityNone &&
		base.CString
}

func (v *Version24Parser) Version() VersionDefinition {
	return VersionDefinition{Major: 2, Minor: 4, Binary: record.BinaryMongod}
}

func (v Version24Parser) parse24WithPayload(r util.RuneReader, command bool) (record.MsgOperationLegacy, error) {
	// command test.$cmd command: { getlasterror: 1.0, w: 1.0 } ntoreturn:1 keyUpdates:0  reslen:67 0ms
	// query test.foo query: { b: 1.0 } ntoreturn:0 ntoskip:0 nscanned:10 keyUpdates:0 locks(micros) r:146 nreturned:1 reslen:64 0ms
	// update vcm_audit.payload.files query: { _id: ObjectId('000000000000000000000000') } update: { _id: ObjectId('000000000000000000000000') } idhack:1 nupdated:1 upsert:1 keyUpdates:0 locks(micros) w:33688 194ms
	op := record.MakeMsgOperationLegacy()
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()

	var target = op.Counters
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		if param[len(param)-1] == ':' {
			param = param[:len(param)-1]
			if op.Operation == "" {
				op.Operation = param
			}
			if r.ExpectRune('{') {
				if command {
					// Commands place the operation name at the beginning of the
					// JSON object.
					r.SlurpWord()
					word := r.PreviewWord(1)
					op.Operation = word[:len(word)-1]

					r.RewindSlurpWord()
				}
				if payload, err := mongo.ParseJsonRunes(&r, false); err != nil {
					return op, err
				} else if command {
					op.Payload = payload
				} else {
					op.Payload[param] = payload
				}
			}
			// For whatever reason, numYields has a space between it and the
			// number (e.g. "numYields: 5").
			if !util.IsNumericRune(r.NextRune()) {
				continue
			} else {
				// Re-add so the parseIntegerKeyValueErratic will succeed.
				param = param + ":"
			}
		}
		v.parseIntegerKeyValueErratic(param, target, &r)
		if param == "locks(micros)" {
			target = op.Locks
		} else if strings.HasSuffix(param, "ms") {
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-2], 10, 64)
		}
	}

	return op, nil
}
func (Version24Parser) parse24WithoutPayload(r util.RuneReader) (record.MsgOperationLegacy, error) {
	// insert test.system.indexes ninserted:1 keyUpdates:0 locks(micros) w:10527 10ms
	op := record.MakeMsgOperationLegacy()
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()
	target := op.Counters
	for param, ok := r.SlurpWord(); ok && param != ""; param, ok = r.SlurpWord() {
		if param == "locks(micros)" {
			target = op.Locks
			continue
		} else if param == "locks:{" {
			// Wrong version, so exit.
			return record.MsgOperationLegacy{}, errors.VersionUnmatched{}
		}
		logger.IntegerKeyValue(param, target, mongo.COUNTERS)
	}
	if dur, ok := r.SlurpWord(); ok && strings.HasSuffix(dur, "ms") {
		op.Duration, _ = strconv.ParseInt(dur[0:len(dur)-3], 10, 64)
	}
	return op, nil
}

func (Version24Parser) parseIntegerKeyValueErratic(param string, target map[string]int64, r *util.RuneReader) {
	if !logger.IntegerKeyValue(param, target, mongo.COUNTERS) && param[len(param)-1] == ':' {
		param = param[0 : len(param)-1]
		if num, err := strconv.ParseInt(r.PreviewWord(1), 10, 64); err == nil {
			if _, ok := mongo.COUNTERS[param]; ok {
				target[mongo.COUNTERS[param]] = num
				r.SlurpWord()
			} else {
				panic("unexpected counter type " + param + " found")
			}
		}
	}
}
