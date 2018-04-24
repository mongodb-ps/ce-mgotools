package parser

import (
	"mgotools/mongo"
	"mgotools/record"
	"mgotools/util"
)

const (
	LOG_VERSION_ANY = BinaryType(iota)
	LOG_VERSION_MONGOD
	LOG_VERSION_MONGOS
)

func (v *VersionCommon) ParseControl(r util.RuneReader, entry record.Entry) (record.Message, error) {
	switch entry.Context {
	case "initandlisten":
		switch {
		case r.ExpectString("build info"):
			return record.MsgBuildInfo{BuildInfo: r.SkipWords(2).Remainder()}, nil
		case r.ExpectString("db version"):
			return parseVersion(r.SkipWords(2).Remainder(), "mongod")
		case r.ExpectString("MongoDB starting"):
			return parseStartupInfo(entry.RawMessage)
		case r.ExpectString("OpenSSL version"):
			return parseVersion(r.SkipWords(2).Remainder(), "OpenSSL")
		case r.ExpectString("options"):
			r.SkipWords(1)
			return parseStartupOptions(r.Remainder())
		}
	case "signalProcessingThread":
		if r.ExpectString("dbexit") {
			return record.MsgShutdown{String: r.Remainder()}, nil
		} else {
			return record.MsgSignal{r.Remainder()}, nil
		}
	}
	return nil, VersionErrorUnmatched{Message: "unrecognized control message"}
}

func (v *VersionCommon) ParseDDL(r util.RuneReader, entry record.Entry) (record.Message, error) {
	if entry.Connection > 0 {
		switch {
		case r.ExpectString("CMD: drop"):
			if namespace, ok := r.SkipWords(2).SlurpWord(); ok {
				return record.MsgDropCollection{namespace}, nil
			}
		case r.ExpectString("dropDatabase"):
			if database, ok := r.SkipWords(1).SlurpWord(); ok {
				if r.NextRune() == '-' {
					r.SkipWords(1)
				}
				return record.MsgDropDatabase{database, r.Remainder()}, nil
			}
		}
	}
	return nil, VersionErrorUnmatched{Message: "unrecognized ddl message"}
}

func (v *VersionCommon) ParseNetwork(r util.RuneReader, entry record.Entry) (record.Message, error) {
	if entry.Connection == 0 {
		if r.ExpectString("connection accepted") { // connection accepted from <IP>:<PORT> #<CONN>
			if addr, port, conn, ok := parseConnectionInit(r.SkipWords(2)); ok {
				return record.MsgConnection{Address: addr, Port: port, Conn: conn, Opened: true}, nil
			}
		} else if r.ExpectString("waiting for connections") {
			return record.MsgListening{}, nil
		} else if entry.Context == "signalProcessingThread" {
			return record.MsgSignal{entry.RawMessage}, nil
		}
	} else if entry.Connection > 0 {
		if r.ExpectString("end connection") {
			if addr, port, _, ok := parseConnectionInit(&r); ok {
				return record.MsgConnection{Address: addr, Port: port, Conn: entry.Connection, Opened: false}, nil
			}
		} else if r.ExpectString("received client metadata from") {
			// Skip "received client metadata" and grab connection information.
			if addr, port, conn, ok := parseConnectionInit(r.SkipWords(3)); !ok {
				return nil, VersionErrorUnmatched{"unexpected connection meta format"}
			} else {
				if meta, err := mongo.ParseJsonRunes(&r, false); err == nil {
					return record.MsgConnectionMeta{record.MsgConnection{addr, conn, port, true}, meta}, nil
				}
			}
		}
	}
	return nil, VersionErrorUnmatched{"unrecognized network message"}
}

func (v *VersionCommon) ParseStorage(r util.RuneReader, entry record.Entry) (record.Message, error) {
	switch entry.Context {
	case "signalProcessingThread":
		return record.MsgSignal{entry.RawMessage}, nil
	case "initandlisten":
		if r.ExpectString("wiredtiger_open config") {
			return record.MsgWiredTigerConfig{String: r.SkipWords(2).Remainder()}, nil
		}
	}
	return nil, VersionErrorUnmatched{"unrecognized storage option"}
}

func (v *VersionCommon) NewLogMessage(entry record.Entry) (record.Message, error) {
	panic("unimplemented call to VersionCommon::NewLogMessage")
}
