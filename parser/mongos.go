package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
)

type mongos record.Entry

func S(entry record.Entry) mongos {
	return mongos(entry)
}

func (entry mongos) Control(r internal.RuneReader) (message.Message, error) {
	switch entry.Context {
	case "mongosMain":
		if r.ExpectString("MongoS version") {
			versionString, ok := r.SkipWords(2).SlurpWord()
			if !ok {
				return nil, internal.UnexpectedVersionFormat
			}

			version, err := makeVersion(versionString, "mongos")
			if err != nil {
				return nil, err
			} else if err == nil && !r.ExpectString("starting:") {
				return version, nil
			} else if info, err := startupInfo(r.Remainder()); err != nil {
				return nil, err
			} else {
				return message.StartupInfoLegacy{
					Version:     version,
					StartupInfo: info,
				}, nil
			}
		} else if r.ExpectString("options:") {
			return startupOptions(r.SkipWords(1).Remainder())
		} else if r.ExpectString("build info") {
			return message.BuildInfo{BuildInfo: r.SkipWords(2).Remainder()}, nil
		}
	}
	return nil, internal.ControlUnrecognized
}

func (entry mongos) Network(r internal.RuneReader) (message.Message, error) {
	// Network messaging is the same for mongos as mongod.
	return D(record.Entry(entry)).Network(r)
}
