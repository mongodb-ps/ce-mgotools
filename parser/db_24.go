package parser

import (
	"strconv"
	"strings"

	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser/message"
	"mgotools/parser/record"
	"mgotools/parser/version"
)

type Version24Parser struct {
	counters []string
}

var errorVersion24Unmatched = internal.VersionUnmatched{Message: "version 2.4"}

func init() {
	version.Factory.Register(func() version.Parser {
		return &Version24Parser{
			// A binary searchable (i.e. sorted) list of counters.
			counters: []string{
				"cursorid",
				"exhaust",
				"idhack",
				"keyUpdates",
				"fastmod",
				"fastmodinsert",
				"ndeleted",
				"ninserted",
				"nmoved",
				"nscanned",
				"ntoreturn",
				"ntoskip",
				"numYields",
				"nupdated",
				"scanAndOrder",
				"upsert",
			},
		}
	})
}

func (v *Version24Parser) NewLogMessage(entry record.Entry) (message.Message, error) {
	r := internal.NewRuneReader(entry.RawMessage)
	switch {
	case entry.Context == "initandlisten", entry.Context == "signalProcessingThread":
		// Check for control messages, which is almost everything in 2.4 that is logged at startup.
		if msg, err := D(entry).Control(*r); err == nil {
			// Most startup messages are part of control.
			return msg, nil
		} else if msg, err := D(entry).Network(*r); err == nil {
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

			if crud, ok := Crud(op.Operation, op.Counters, op.Payload); ok {
				if op.Operation == "query" {
					// Standardize with newer versions to queries appear as finds.
					op.Operation = "find"
				}

				crud.Message = op
				return crud, nil
			}

			return op, nil

		case r.ExpectString("command"):
			// Commands in 2.4 don't include anything that should be converted
			// into operations (e.g. find, update, remove, etc).
			if op, err := v.parse24WithPayload(r, true); err != nil {
				return nil, err
			} else {
				cmd := message.MakeCommandLegacy()
				cmd.Command = op.Operation
				cmd.Duration = op.Duration
				cmd.Namespace = NamespaceReplace(op.Operation, op.Payload, op.Namespace)
				cmd.Locks = op.Locks
				cmd.Payload = op.Payload

				return CrudOrMessage(op, op.Operation, op.Counters, op.Payload), nil
			}

		case r.ExpectString("insert"):
			// Inserts!
			return v.parse24WithoutPayload(r)

		case r.ExpectString("getmore"):
			op, err := v.parse24WithoutPayload(r)
			if err != nil {
				return nil, err
			}

			return CrudOrMessage(op, op.Operation, op.Counters, op.Payload), nil

		default:
			// Check for network connection changes.
			if msg, err := D(entry).Network(*r); err == nil {
				return msg, nil
			}
		}
	}
	return nil, errorVersion24Unmatched
}

func (v *Version24Parser) Check(base record.Base) bool {
	return base.Component == record.ComponentNone &&
		base.Severity == record.SeverityNone &&
		base.CString
}

func (v *Version24Parser) Version() version.Definition {
	return version.Definition{Major: 2, Minor: 4, Binary: record.BinaryMongod}
}

func (v Version24Parser) parse24WithPayload(r *internal.RuneReader, command bool) (message.OperationLegacy, error) {
	// command test.$cmd command: { getlasterror: 1.0, w: 1.0 } ntoreturn:1 keyUpdates:0  reslen:67 0ms
	// query test.foo query: { b: 1.0 } ntoreturn:0 ntoskip:0 nscanned:10 keyUpdates:0 locks(micros) r:146 nreturned:1 reslen:64 0ms
	// update vcm_audit.payload.files query: { _id: ObjectId('000000000000000000000000') } update: { _id: ObjectId('000000000000000000000000') } idhack:1 nupdated:1 upsert:1 keyUpdates:0 locks(micros) w:33688 194ms
	op := message.MakeOperationLegacy()
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()

	if cmd := r.PreviewWord(1); cmd == "" {
		return message.OperationLegacy{}, internal.UnexpectedEOL
	} else if cmd != "command:" && command {
		return message.OperationLegacy{}, internal.CommandStructure
	} else if cmd != "query:" && !command {
		return message.OperationLegacy{}, internal.OperationStructure
	}

	// Define the target for key:value finds (start with counters and end with
	// locks) since both counters and locks look the same in this version.
	var target = op.Counters

	// Iterate through each word in the line.
ParamLoop:
	for param, ok := r.SlurpWord(); ok; param, ok = r.SlurpWord() {
		if param[len(param)-1] == ':' {
			param = param[:len(param)-1]
			if op.Operation == "" {
				op.Operation = param
			}
			if r.ExpectRune('{') {
				if command {
					// Commands place the operation name before the JSON object.
					r.SlurpWord()
					word := r.PreviewWord(1)
					op.Operation = word[:len(word)-1]

					r.RewindSlurpWord()
				}

				if payload, err := mongo.ParseJsonRunes(r, false); err != nil {
					if !command {
						// An issue parsing runes could be caused by any number
						// of problems. But there is a subset of cases that can be
						// ignored. We only care about ignoring this subset if
						// a query already exists since at least part of the line
						// may be useful.

						if _, ok := op.Payload["query"]; ok {
							// A query exists so continue forward until we
							// find something that looks like a key:value.
							for ; ok; param, ok = r.SlurpWord() {
								if key, _, ok := internal.StringDoubleSplit(param, ':'); ok {
									if internal.ArrayBinaryMatchString(key, v.counters) {
										// I dislike using labels, but it's
										// quick, easy, and perfectly fine here.
										continue ParamLoop
									}
								}
							}
						}
					}

					// Otherwise, there's a problem.
					return op, err
				} else if command {
					op.Payload = payload
				} else {
					op.Payload[param] = payload
				}
			}
			// For whatever reason, numYields has a space between it and the
			// number (e.g. "numYields: 5").
			if !internal.IsNumericRune(r.NextRune()) {
				continue
			} else {
				// Re-add so the parseIntegerKeyValueErratic will succeed.
				param = param + ":"
			}
		}
		v.parseIntegerKeyValueErratic(param, target, r)
		if param == "locks(micros)" {
			target = op.Locks
		} else if strings.HasSuffix(param, "ms") {
			op.Duration, _ = strconv.ParseInt(param[0:len(param)-2], 10, 64)
		}
	}

	return op, nil
}
func (Version24Parser) parse24WithoutPayload(r *internal.RuneReader) (message.OperationLegacy, error) {
	// insert test.system.indexes ninserted:1 keyUpdates:0 locks(micros) w:10527 10ms
	op := message.MakeOperationLegacy()
	op.Operation, _ = r.SlurpWord()
	op.Namespace, _ = r.SlurpWord()
	target := op.Counters
	for param, ok := r.SlurpWord(); ok; param, ok = r.SlurpWord() {
		if param == "locks(micros)" {
			target = op.Locks
			continue
		} else if param == "locks:{" {
			// Wrong version, so exit.
			return message.OperationLegacy{}, internal.VersionUnmatched{}
		}
		IntegerKeyValue(param, target, record.COUNTERS)
	}
	if dur, ok := r.SlurpWord(); ok && strings.HasSuffix(dur, "ms") {
		op.Duration, _ = strconv.ParseInt(dur[0:len(dur)-3], 10, 64)
	}
	return op, nil
}

func (Version24Parser) parseIntegerKeyValueErratic(param string, target map[string]int64, r *internal.RuneReader) {
	if !IntegerKeyValue(param, target, record.COUNTERS) && param[len(param)-1] == ':' {
		param = param[0 : len(param)-1]
		if num, err := strconv.ParseInt(r.PreviewWord(1), 10, 64); err == nil {
			if _, ok := record.COUNTERS[param]; ok {
				target[record.COUNTERS[param]] = num
				r.SlurpWord()
			} else {
				panic("unexpected counter type " + param + " found")
			}
		}
	}
}
