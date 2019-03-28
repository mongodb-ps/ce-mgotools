package parser

import (
	"mgotools/internal"
	"mgotools/record"
	"mgotools/util"
)

type mongos record.Entry

func S(entry record.Entry) mongos {
	return mongos(entry)
}

func (entry mongos) Control(r *util.RuneReader) (record.Message, error) {
	switch entry.Context {
	case "mongosMain":
	case "mongoS":
		if r.ExpectString("MongoS version") {
			return version(r.SkipWords(2).Remainder(), "mongos")
		} else if r.ExpectString("options:") {
			return startupOptions(r.SkipWords(1).Remainder())
		}
	}
	return nil, internal.ControlUnrecognized
}
