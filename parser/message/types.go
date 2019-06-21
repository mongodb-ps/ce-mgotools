package message

import "net"

type Message interface {
}

type Authentication struct {
	Principal string
	IP        string
}

type BuildInfo struct {
	BuildInfo string
}

type Connection struct {
	Address net.IP
	Conn    int
	Port    uint16
	Opened  bool

	Exception string
}

type ConnectionMeta struct {
	Connection
	Meta interface{}
}

type Empty struct{}

type Listening struct{}

type OpenSSL struct {
	String string
}

type Shutdown struct {
	String string
}

type Signal struct {
	String string
}

type StartupInfoLegacy struct {
	StartupInfo
	Version
}

type StartupInfo struct {
	DbPath   string
	Hostname string
	Pid      int
	Port     int
}

type StartupOptions struct {
	String  string
	Options interface{}
}

type Version struct {
	Binary   string
	Major    int
	Minor    int
	Revision int
}

type WiredTigerConfig struct {
	String string
}

//
// Message Commands
//

type BaseCommand struct {
	Counters    map[string]int64
	Duration    int64
	Exception   string
	Namespace   string
	PlanSummary []PlanSummary
}

type Payload map[string]interface{}

type Command struct {
	BaseCommand

	Agent    string
	Command  string
	Locks    map[string]interface{}
	Payload  Payload
	Protocol string
	Storage  map[string]interface{}
}

// remove, update, query, insert
type Operation struct {
	BaseCommand

	Agent     string
	Locks     map[string]interface{}
	Operation string
	Payload   Payload
	Storage   map[string]interface{}
}

type CommandLegacy struct {
	BaseCommand

	Command string
	Locks   map[string]int64
	Payload Payload
}

type OperationLegacy struct {
	BaseCommand

	Locks     map[string]int64
	Operation string
	Payload   Payload
}

type Filter map[string]interface{}
type Project map[string]interface{}
type Sort map[string]interface{}
type Update map[string]interface{}

type PlanSummary struct {
	Type string
	Key  interface{}
}

type CRUD struct {
	Message

	Comment  string
	CursorId int64
	Filter   Filter
	N        int64
	Project  Project
	Sort     Sort
	Update   Update
}
