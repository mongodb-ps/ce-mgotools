package parser

import (
	"mgotools/log"
	"mgotools/mongo"
	"mgotools/util"
)

const (
	LOG_VERSION_ANY = iota
	LOG_VERSION_MONGOD
	LOG_VERSION_MONGOS
)

func (v *LogVersionCommon) ParseControl(r util.RuneReader, entry log.Entry) (log.Message, error) {
	switch entry.Context {
	case "initandlisten":
		switch {
		case r.ExpectString("build info"):
			return log.MsgBuildInfo{BuildInfo: r.SkipWords(2).Remainder()}, nil
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
			return log.MsgShutdown{String: r.Remainder()}, nil
		} else {
			return log.MsgSignal{r.Remainder()}, nil
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "unrecognized control message"}
}

func (v *LogVersionCommon) ParseDDL(r util.RuneReader, entry log.Entry) (log.Message, error) {
	if entry.Connection > 0 {
		switch {
		case r.ExpectString("CMD: drop"):
			if namespace, ok := r.SkipWords(2).SlurpWord(); ok {
				return log.MsgDropCollection{namespace}, nil
			}
		case r.ExpectString("dropDatabase"):
			if database, ok := r.SkipWords(1).SlurpWord(); ok {
				if r.NextRune() == '-' {
					r.SkipWords(1)
				}
				return log.MsgDropDatabase{database, r.Remainder()}, nil
			}
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "unrecognized ddl message"}
}

func (v *LogVersionCommon) ParseNetwork(r util.RuneReader, entry log.Entry) (log.Message, error) {
	if entry.Connection == 0 {
		if r.ExpectString("connection accepted") { // connection accepted from <IP>:<PORT> #<CONN>
			if addr, port, conn, ok := parseConnectionInit(r.SkipWords(2)); ok {
				return log.MsgConnection{Address: addr, Port: port, Conn: conn, Opened: true}, nil
			}
		} else if r.ExpectString("waiting for connections") {
			return log.MsgListening{}, nil
		} else if entry.Context == "signalProcessingThread" {
			return log.MsgSignal{entry.RawMessage}, nil
		}
	} else if entry.Connection > 0 {
		if r.ExpectString("end connection") {
			if addr, port, _, ok := parseConnectionInit(&r); ok {
				return log.MsgConnection{Address: addr, Port: port, Conn: entry.Connection, Opened: false}, nil
			}
		} else if r.ExpectString("received client metadata from") {
			// Skip "received client metadata" and grab connection information.
			if addr, port, conn, ok := parseConnectionInit(r.SkipWords(3)); !ok {
				return nil, LogVersionErrorUnmatched{"unexpected connection meta format"}
			} else {
				if meta, err := mongo.ParseJsonRunes(&r, false); err == nil {
					return log.MsgConnectionMeta{log.MsgConnection{addr, conn, port, true}, meta}, nil
				}
			}
		}
	}
	return nil, LogVersionErrorUnmatched{"unrecognized network message"}
}

func (v *LogVersionCommon) ParseStorage(r util.RuneReader, entry log.Entry) (log.Message, error) {
	switch entry.Context {
	case "signalProcessingThread":
		return log.MsgSignal{entry.RawMessage}, nil
	case "initandlisten":
		if r.ExpectString("wiredtiger_open config") {
			return log.MsgWiredTigerConfig{String: r.SkipWords(2).Remainder()}, nil
		}
	}
	return nil, LogVersionErrorUnmatched{"unrecognized storage option"}
}

func (v *LogVersionCommon) NewLogMessage(entry log.Entry) (log.Message, error) {
	panic("unimplemented call to LogVersionCommon::NewLogMessage")
}
func (e LogVersionErrorUnmatched) Error() string {
	if e.Message != "" {
		return "Log message not recognized: " + e.Message
	} else {
		return "Log message not recognized"
	}
}
