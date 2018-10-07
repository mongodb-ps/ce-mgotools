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

type MsgNamespace struct {
	Namespace string
}

type MsgCollectionIndexOperation struct {
	MsgNamespace
	MsgPayload

	Operation  string
	Properties map[string]interface{}
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
	CRUD    *MsgCRUD
	Locks   map[string]interface{}
	Payload MsgPayload
}

// remove, update, query, insert
type MsgOperation struct {
	MsgWireProtocol
	MsgBase

	CRUD      *MsgCRUD
	Locks     map[string]interface{}
	Operation string
	Payload   MsgPayload
}

type MsgCommandLegacy struct {
	MsgBase

	Command string
	CRUD    *MsgCRUD
	Locks   map[string]int
	Payload MsgPayload
}

type MsgOperationLegacy struct {
	MsgBase

	CRUD      *MsgCRUD
	Locks     map[string]int
	Operation string
	Payload   MsgPayload
}

type MsgBase struct {
	MsgNamespace

	Counters    map[string]int
	Duration    int64
	Errors      []error
	PlanSummary []MsgPlanSummary
}

type MsgFilter map[string]interface{}
type MsgSort map[string]interface{}

type MsgPlanSummary struct {
	Type    string
	Summary interface{}
}

type MsgCRUD struct {
	Filter  MsgFilter
	Sort    MsgSort
	Comment string
	N       int
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

func MsgNamespaceFromMessage(msg Message) (*MsgNamespace, bool) {
	if msg == nil {
		return &MsgNamespace{}, false
	}
	switch t := msg.(type) {
	case MsgNamespace:
		return &t, true
	case MsgCommand:
		return &t.MsgNamespace, true
	case MsgCommandLegacy:
		return &t.MsgNamespace, true
	default:
		return &MsgNamespace{}, false
	}
}

func MakeMsgCommand() MsgCommand {
	return MsgCommand{
		MsgBase: MsgBase{
			Counters: make(map[string]int),
		},
		Payload: make(map[string]interface{}),
		Locks:   make(map[string]interface{}),
	}
}

func MakeMsgOperation() MsgOperation {
	return MsgOperation{
		MsgBase: MsgBase{
			Counters: make(map[string]int),
		},
		Payload: make(map[string]interface{}),
		Locks:   make(map[string]interface{}),
	}
}

func MakeMsgCommandLegacy() MsgCommandLegacy {
	return MsgCommandLegacy{
		MsgBase: MsgBase{
			Counters: make(map[string]int),
		},
		Payload: make(map[string]interface{}),
		Locks:   make(map[string]int),
	}
}

func MakeMsgOperationLegacy() MsgOperationLegacy {
	return MsgOperationLegacy{
		MsgBase: MsgBase{
			Counters: make(map[string]int),
		},
		Payload: make(map[string]interface{}),
		Locks:   make(map[string]int),
	}
}
