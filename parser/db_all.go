package parser

import (
	"mgotools/util"
	"time"
)

const (
	LOG_VERSION_MONGOD = iota
	LOG_VERSION_MONGOS
)

type LogVersionParser interface {
	NewLogMessage(LogEntry) (LogMessage, error)
	ParseDate(string) (time.Time, error)
	Version() LogVersionDefinition
}
type LogVersionCommon struct {
	*util.DateParser
}
type LogVersionDateUnmatched struct {
	Message string
}
type LogVersionDefinition struct {
	Major int
	Minor int
	Binary int
}
type LogVersionErrorUnmatched struct {
	Message string
}
func (v *LogVersionCommon) NewLogMessage(entry LogEntry) (LogMessage, error) {
	if entry.LogMessage == "" {
		return LogMsgEmpty{}, nil
	}
	msg := util.NewRuneReader(entry.RawMessage)
	switch {
	case entry.Context == "initandlisten":
		switch {
		case msg.ExpectString("build info"):
			return &LogMsgBuildInfo{BuildInfo: msg.SkipWords(2).Remainder()}, nil
		case msg.ExpectString("connection accepted"): // connection accepted from <IP>:<PORT> #<CONN>
			if addr, port, conn, ok := parseConnectionInit(msg.SkipWords(2)); ok {
				return LogMsgConnection{Address: addr, Port: port, Conn: conn, Opened: true}, nil
			}
		case msg.ExpectString("db version"):
			return parseVersion(msg.SkipWords(2).Remainder(), "mongod")
		case msg.ExpectString("MongoDB starting"):
			return parseStartupInfo(entry.RawMessage)
		case msg.ExpectString("OpenSSL version"):
			return parseVersion(msg.SkipWords(2).Remainder(), "OpenSSL")
		case msg.ExpectString("options"):
			msg.SkipWords(1)
			return parseStartupOptions(msg.Remainder())
		case msg.ExpectString("wiredtiger_open config"):
			return LogMsgWiredTigerConfig{String: msg.SkipWords(2).Remainder()}, nil
		case msg.ExpectString("waiting for connections"):
			return LogMsgListening{}, nil
		}
	case entry.Connection > 0:
		switch {
		case msg.ExpectString("end connection"):
			if addr, port, _, ok := parseConnectionInit(msg); ok {
				return LogMsgConnection{Address: addr, Port: port, Conn: entry.Connection, Opened: false}, nil
			}
		}
	default:
		switch {
		case msg.ExpectString("dbexit"):
			return LogMsgShutdown{String: msg.Remainder()}, nil
		}
	}
	return "", LogVersionErrorUnmatched{Message: entry.RawMessage}
}
func (e LogVersionErrorUnmatched) Error() string {
	if e.Message != "" {
		return "Log message not recognized: " + e.Message
	} else {
		return "Log message not recognized"
	}
}
