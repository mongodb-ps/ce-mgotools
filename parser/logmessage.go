package parser

import (
	"net"
)

const (
	LOG_MESSAGE_TYPE_GENERAL    = iota
	LOG_MESSAGE_TYPE_COMMAND
	LOG_MESSAGE_TYPE_CONNECTION
)

type LogMessage interface {
}

type LogMsgBuildInfo struct {
	BuildInfo string
}

type LogMsgConnection struct {
	Address net.IP
	Conn    int
	Port    uint16
	Opened  bool
}

type LogMsgEmpty struct{}

type LogMsgListening struct{}

type LogMsgOpenSSL struct {
	String string
}

type LogMsgOperation struct {
	Operation string
	Namespace string
}

type LogMsgOpIndex struct {
	LogMsgOperation
	Properties map[string]interface{}
}

type LogMsgOpCommand struct {
	LogMsgOperation
	SubOperation string
	Command      map[string]interface{}
	Counters     map[string]int
	Locks        map[string]interface{}
	Duration     int64
}

type LogMsgOpCommandLegacy struct {
	LogMsgOperation
	Command  map[string]interface{}
	Counters map[string]int
	Locks    map[string]int
	Duration int64
}

type LogMsgShutdown struct {
	String string
}

type LogMsgStartupInfo struct {
	DbPath   string
	Hostname string
	Pid      int
	Port     int
}

type LogMsgStartupOptions struct {
	String  string
	Options interface{}
}

type LogMsgVersion struct {
	Binary   string
	Major    int
	Minor    int
	Revision int
	String   string
}

type LogMsgWiredTigerConfig struct {
	String string
}
