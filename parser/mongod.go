package parser

import (
	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser/message"
	"mgotools/parser/record"
)

type mongod record.Entry

func D(entry record.Entry) mongod {
	return mongod(entry)
}

func (entry mongod) Control(r internal.RuneReader) (message.Message, error) {
	switch entry.Context {
	case "initandlisten":
		switch {
		case r.ExpectString("build info"):
			return message.BuildInfo{BuildInfo: r.SkipWords(2).Remainder()}, nil

		case r.ExpectString("db version"):
			return makeVersion(r.SkipWords(2).Remainder(), "mongod")

		case r.ExpectString("MongoDB starting"):
			return startupInfo(entry.RawMessage)

		case r.ExpectString("options"):
			r.SkipWords(1)
			return startupOptions(r.Remainder())
		}

	case "signalProcessingThread":
		if r.ExpectString("dbexit") {
			return message.Shutdown{String: r.Remainder()}, nil
		} else {
			return message.Signal{String: r.Remainder()}, nil
		}
	}

	return nil, internal.ControlUnrecognized
}

func (entry mongod) Network(r internal.RuneReader) (message.Message, error) {
	if entry.Connection == 0 {
		if r.ExpectString("connection accepted") { // connection accepted from <IP>:<PORT> #<CONN>
			if addr, port, conn, ok := connectionInit(r.SkipWords(3)); ok {
				return message.Connection{Address: addr, Port: port, Conn: conn, Opened: true}, nil
			}
		} else if r.ExpectString("waiting for connections") {
			return message.Listening{}, nil
		} else if entry.Context == "signalProcessingThread" {
			return message.Signal{String: entry.RawMessage}, nil
		}
	} else if entry.Connection > 0 {
		if r.ExpectString("end connection") {
			if addr, port, ok := connectionTerminate(r.SkipWords(2)); ok {
				return message.Connection{Address: addr, Port: port, Conn: entry.Connection, Opened: false}, nil
			}
		} else if r.ExpectString("received client metadata from") {
			// Skip "received client metadata" and grab connection information.
			if addr, port, conn, ok := connectionInit(r.SkipWords(4)); !ok {
				return nil, internal.MetadataUnmatched
			} else {
				if meta, err := mongo.ParseJsonRunes(&r, false); err == nil {
					return message.ConnectionMeta{
						Connection: message.Connection{
							Address: addr,
							Conn:    conn,
							Port:    port,
							Opened:  true},
						Meta: meta}, nil
				}
			}
		}
	}

	return nil, internal.NetworkUnrecognized
}

func (entry mongod) Storage(r internal.RuneReader) (message.Message, error) {
	switch entry.Context {
	case "signalProcessingThread":
		return message.Signal{entry.RawMessage}, nil

	case "initandlisten":
		if r.ExpectString("wiredtiger_open config") {
			return message.WiredTigerConfig{String: r.SkipWords(2).Remainder()}, nil
		}
	}

	return nil, internal.StorageUnmatched
}
