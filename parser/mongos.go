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

func (entry mongos) Control(r util.RuneReader) (record.Message, error) {
	switch entry.Context {
	case "mongosMain":
		if r.ExpectString("MongoS version") {
			return version(r.SkipWords(2).Remainder(), "mongos")
		} else if r.ExpectString("options:") {
			return startupOptions(r.SkipWords(1).Remainder())
		} else if r.ExpectString("build info") {
			return record.MsgBuildInfo{BuildInfo: r.SkipWords(2).Remainder()}, nil
		}
	}
	return nil, internal.ControlUnrecognized
}

func (entry mongos) Network(r util.RuneReader) (record.Message, error) {
	// Network messaging is the same for mongos as mongod.
	return D(record.Entry(entry)).Network(r)
}
