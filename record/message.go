package record

import (
	"net"
)

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

type MsgEmpty struct{}

type MsgListening struct{}

type MsgOpenSSL struct {
	String string
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

//
// Message Commands
//

type MsgBase struct {
	Counters    map[string]int64
	Duration    int64
	Exception   string
	Namespace   string
	PlanSummary []MsgPlanSummary
}

type MsgPayload map[string]interface{}

type MsgWireProtocol struct {
	Agent    string
	Protocol string
}

type MsgCommand struct {
	MsgBase
	MsgWireProtocol

	Command string
	Locks   map[string]interface{}
	Payload MsgPayload
}

// remove, update, query, insert
type MsgOperation struct {
	MsgWireProtocol
	MsgBase

	Locks     map[string]interface{}
	Operation string
	Payload   MsgPayload
}

type MsgCommandLegacy struct {
	MsgBase

	Command string
	Locks   map[string]int64
	Payload MsgPayload
}

type MsgOperationLegacy struct {
	MsgBase

	Locks     map[string]int64
	Operation string
	Payload   MsgPayload
}

type MsgFilter map[string]interface{}
type MsgProject map[string]interface{}
type MsgSort map[string]interface{}
type MsgUpdate map[string]interface{}

type MsgPlanSummary struct {
	Type string
	Key  interface{}
}

type MsgCRUD struct {
	Message

	Comment  string
	CursorId int64
	Filter   MsgFilter
	N        int64
	Project  MsgProject
	Sort     MsgSort
	Update   MsgUpdate
}

func MsgBaseFromMessage(msg Message) (*MsgBase, bool) {
	if msg == nil {
		return &MsgBase{}, false
	}
	switch t := msg.(type) {
	case MsgBase:
		return &t, true
	case MsgCommand:
		return &t.MsgBase, true
	case MsgCommandLegacy:
		return &t.MsgBase, true
	case MsgOperation:
		return &t.MsgBase, true
	case MsgOperationLegacy:
		return &t.MsgBase, true
	default:
		return &MsgBase{}, false
	}
}

func MsgPayloadFromMessage(msg Message) (*MsgPayload, bool) {
	if msg == nil {
		return &MsgPayload{}, false
	}
	switch t := msg.(type) {
	case MsgCommand:
		return &t.Payload, true
	case MsgCommandLegacy:
		return &t.Payload, true
	case MsgOperation:
		return &t.Payload, true
	case MsgOperationLegacy:
		return &t.Payload, true
	default:
		return &MsgPayload{}, false
	}
}

func MakeMsgCommand() MsgCommand {
	return MsgCommand{
		MsgBase: MsgBase{
			Counters: make(map[string]int64),
		},
		Command: "",
		Payload: make(MsgPayload),
		Locks:   make(map[string]interface{}),
	}
}

func MakeMsgOperation() MsgOperation {
	return MsgOperation{
		MsgBase: MsgBase{
			Counters: make(map[string]int64),
		},
		Operation: "",
		Payload:   make(MsgPayload),
		Locks:     make(map[string]interface{}),
	}
}

func MakeMsgCommandLegacy() MsgCommandLegacy {
	return MsgCommandLegacy{
		MsgBase: MsgBase{
			Counters: make(map[string]int64),
		},
		Command: "",
		Payload: make(MsgPayload),
		Locks:   make(map[string]int64),
	}
}

func MakeMsgOperationLegacy() MsgOperationLegacy {
	return MsgOperationLegacy{
		MsgBase: MsgBase{
			Counters: make(map[string]int64),
		},
		Operation: "",
		Payload:   make(MsgPayload),
		Locks:     make(map[string]int64),
	}
}
