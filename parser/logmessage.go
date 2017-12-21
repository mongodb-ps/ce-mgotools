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

type LogMsgConnectionMeta struct {
	LogMsgConnection
	Meta interface{}
}

type LogMsgDropCollection struct {
	Namespace string
}

type LogMsgDropDatabase struct {
	Database string
	Note     string
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

type LogMsgOpCommandBase struct {
	Command     map[string]interface{}
	Counters    map[string]int
	Duration    int64
	Errors      []error
	PlanSummary []LogMsgOpCommandPlanSummary
}

type LogMsgOpCommandWP struct {
	Agent    string
	Protocol string
}

type LogMsgOpCommand struct {
	LogMsgOperation
	LogMsgOpCommandBase
	LogMsgOpCommandWP
	SubOperation string
	Locks        map[string]interface{}
}

type LogMsgOpCommandLegacy struct {
	LogMsgOperation
	LogMsgOpCommandBase
	Locks map[string]int
}

type LogMsgOpCommandPlanSummary struct {
	Type    string
	Summary interface{}
}

type LogMsgShutdown struct {
	String string
}

type LogMsgSignal struct {
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

func MakeLogMsgOpCommand() LogMsgOpCommand {
	return LogMsgOpCommand{
		LogMsgOpCommandBase: LogMsgOpCommandBase{
			Command:  make(map[string]interface{}),
			Counters: make(map[string]int),
		},
		Locks: make(map[string]interface{}),
	}
}

func MakeLogMsgOpCommandLegacy() LogMsgOpCommandLegacy {
	return LogMsgOpCommandLegacy{
		LogMsgOpCommandBase: LogMsgOpCommandBase{
			Command:  make(map[string]interface{}),
			Counters: make(map[string]int),
		},
		Locks: make(map[string]int),
	}
}
