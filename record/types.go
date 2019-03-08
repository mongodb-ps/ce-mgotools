package record

import "net"

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

	Exception string
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

type MsgCommand struct {
	MsgBase

	Agent    string
	Command  string
	Locks    map[string]interface{}
	Payload  MsgPayload
	Protocol string
}

// remove, update, query, insert
type MsgOperation struct {
	MsgBase

	Agent     string
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
