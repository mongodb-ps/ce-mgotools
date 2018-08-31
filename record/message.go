package record

import (
	"net"
)

const (
	LOG_MESSAGE_TYPE_GENERAL    = iota
	LOG_MESSAGE_TYPE_COMMAND
	LOG_MESSAGE_TYPE_CONNECTION
)

type Filter map[string]interface{}

func (m Filter) IsSet() bool {
	return m != nil
}

type Message interface {
}

type MsgBuildInfo struct {
	BuildInfo string
}

type MsgConnection struct {
	Address net.IP
	Conn    int
	Port    uint16
	Opened  bool
}

type MsgConnectionMeta struct {
	MsgConnection
	Meta interface{}
}

type MsgCreateCollection struct {
	MsgOpCommandBase
	Name string
}

type MsgDropCollection struct {
	Namespace string
}

type MsgDropDatabase struct {
	Database string
	Note     string
}

type MsgEmpty struct{}

type MsgListening struct{}

type MsgOpenSSL struct {
	String string
}

type MsgOperation struct {
	Operation string
	Namespace string
}

type MsgOpIndex struct {
	MsgOperation
	MsgOpPayload
	Properties map[string]interface{}
}

type MsgOpPayload struct {
	Command     string
	Counters    map[string]int
	Duration    int64
	Errors      []error
	Normalized  bool
	Payload     map[string]interface{}
	PlanSummary []MsgOpCommandPlanSummary
}

type MsgOpCommandWireProtocol struct {
	Agent    string
	Protocol string
}

type MsgOpCommand struct {
	MsgOpCommandBase
	MsgOpCommandWireProtocol
	Locks map[string]interface{}
}

type MsgOpCommandBase struct {
	MsgOperation
	MsgOpPayload
}

type MsgOpCommandLegacy struct {
	MsgOpCommandBase
	Locks map[string]int
}

type MsgOpCommandPlanSummary struct {
	Type    string
	Summary interface{}
}

type MsgShutdown struct {
	String string
}

type MsgSignal struct {
	String string
}

type MsgStartupInfo struct {
	DbPath   string
	Hostname string
	Pid      int
	Port     int
}

type MsgStartupOptions struct {
	String  string
	Options interface{}
}

type MsgVersion struct {
	Binary   string
	Major    int
	Minor    int
	Revision int
	String   string
}

type MsgWiredTigerConfig struct {
	String string
}

func MsgOpCommandBaseFromMessage(msg Message) (*MsgOpCommandBase, bool) {
	if msg == nil {
		return &MsgOpCommandBase{}, false
	}
	switch t := msg.(type) {
	case MsgOpCommandBase:
		return &t, true
	case MsgOpCommand:
		return &t.MsgOpCommandBase, true
	case MsgOpCommandLegacy:
		return &t.MsgOpCommandBase, true
	default:
		return &MsgOpCommandBase{}, false
	}
}

func MsgOperationFromMessage(msg Message) (MsgOperation, bool) {
	if msg == nil {
		return MsgOperation{}, false
	}
	switch t := msg.(type) {
	case MsgOperation:
		return t, true
	case MsgOpCommand:
		return t.MsgOperation, true
	case MsgOpCommandLegacy:
		return t.MsgOperation, true
	default:
		return MsgOperation{}, false
	}
}

func MakeMsgOpCommand() MsgOpCommand {
	return MsgOpCommand{
		MsgOpCommandBase: MsgOpCommandBase{
			MsgOpPayload: MsgOpPayload{
				Payload:    make(map[string]interface{}),
				Counters:   make(map[string]int),
				Normalized: false,
			},
		},
		Locks: make(map[string]interface{}),
	}
}

func MakeMsgOpCommandLegacy() MsgOpCommandLegacy {
	return MsgOpCommandLegacy{
		MsgOpCommandBase: MsgOpCommandBase{
			MsgOpPayload: MsgOpPayload{
				Payload:    make(map[string]interface{}),
				Counters:   make(map[string]int),
				Normalized: false,
			},
		},
		Locks: make(map[string]int),
	}
}
