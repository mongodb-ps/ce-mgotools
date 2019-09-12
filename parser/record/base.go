package record

import (
	"mgotools/internal"
)

type Component int

const (
	ComponentNone   Component = 0
	ComponentAccess           = 1 << iota
	ComponentAccessControl
	ComponentASIO
	ComponentBridge
	ComponentCommand
	ComponentConnPool
	ComponentControl
	ComponentDefault
	ComponentElection
	ComponentExecutor
	ComponentFTDC
	ComponentGeo
	ComponentHeartbeats
	ComponentIndex
	ComponentInitialSync
	ComponentJournal
	ComponentNetwork
	ComponentQuery
	ComponentRecovery
	ComponentRepl
	ComponentReplication
	ComponentReplHB
	ComponentRollback
	ComponentSharding
	ComponentShardingRefr
	ComponentStorage
	ComponentTotal
	ComponentTracking
	ComponentWrite
	ComponentUnknown
)

func NewComponent(s string) (Component, bool) {
	switch s {
	case "ACCESS": // 3.0, 3.2, 3.4, 3.6
		return ComponentAccess, true
	case "ACCESSCONTROL": // 3.0, 3.2, 3.4, 3.6
		return ComponentAccessControl, true
	case "ASIO": // 3.2, 3.4, 3.6
		return ComponentASIO, true
	case "BRIDGE": // 3.0, 3.2, 3.4, 3.6
		return ComponentBridge, true
	case "COMMAND": // 3.0, 3.2, 3.4, 3.6
		return ComponentCommand, true
	case "CONNPOOL": // 4.0
		return ComponentConnPool, true
	case "CONTROL": // 3.0, 3.2, 3.4, 3.6
		return ComponentControl, true
	case "DEFAULT": // 3.0, 3.2, 3.4, 3.6
		return ComponentDefault, true
	case "ELECTION": // 4.2
		return ComponentElection, true
	case "EXECUTOR": // 3.2, 3.4, 3.6
		return ComponentExecutor, true
	case "FTDC": // 3.2, 3.4, 3.6
		return ComponentFTDC, true
	case "GEO": // 3.0, 3.2, 3.4, 3.6
		return ComponentGeo, true
	case "HEARTBEATS": // 3.6
		return ComponentHeartbeats, true
	case "INDEX": // 3.0, 3.2, 3.4, 3.6
		return ComponentIndex, true
	case "INITSYNC": // 4.2
		return ComponentInitialSync, true
	case "JOURNAL": // 3.0, 3.2, 3.4, 3.6
		return ComponentJournal, true
	case "NETWORK": // 3.0, 3.2, 3.4, 3.6
		return ComponentNetwork, true
	case "QUERY": // 3.0, 3.2, 3.4, 3.6
		return ComponentQuery, true
	case "RECOVERY": // 4.0
		return ComponentRecovery, true
	case "REPL": // 3.0, 3.2, 3.4, 3.6
		return ComponentRepl, true
	case "REPLICATION": // 3.0, 3.2, 3.4, 3.6
		return ComponentReplication, true
	case "REPL_HB": // 3.6
		return ComponentReplHB, true
	case "ROLLBACK": // 3.6
		return ComponentRollback, true
	case "SHARDING": // 3.0, 3.2, 3.4, 3.6
		return ComponentSharding, true
	case "SH_REFR": // 2.4
		return ComponentShardingRefr, true
	case "STORAGE": // 3.0, 3.2, 3.4, 3.6
		return ComponentStorage, true
	case "TOTAL": // 3.0, 3.2, 3.4, 3.6
		return ComponentTotal, true
	case "TRACKING": // 3.4, 3.6
		return ComponentTracking, true
	case "WRITE": // 3.0, 3.2, 3.4, 3.6
		return ComponentWrite, true
	case "-":
		return ComponentUnknown, true
	default:
		return ComponentNone, false
	}
}

func (c Component) String() string {
	switch c {
	case ComponentNone:
		return ""
	case ComponentAccess:
		return "ACCESS"
	case ComponentAccessControl:
		return "ACCESSCONTROL"
	case ComponentASIO:
		return "ASIO"
	case ComponentBridge:
		return "BRIDGE"
	case ComponentCommand:
		return "COMMAND"
	case ComponentConnPool:
		return "CONNPOOL"
	case ComponentControl:
		return "CONTROL"
	case ComponentDefault:
		return "DEFAULT"
	case ComponentElection:
		return "ELECTION"
	case ComponentExecutor:
		return "EXECUTOR"
	case ComponentFTDC:
		return "FTDC"
	case ComponentGeo:
		return "GEO"
	case ComponentHeartbeats:
		return "HEARTBEATS"
	case ComponentIndex:
		return "INDEX"
	case ComponentInitialSync:
		return "INITSYNC"
	case ComponentJournal:
		return "JOURNAL"
	case ComponentNetwork:
		return "NETWORK"
	case ComponentQuery:
		return "QUERY"
	case ComponentRecovery:
		return "RECOVERY"
	case ComponentRepl:
		return "REPL"
	case ComponentReplication:
		return "REPLICATION"
	case ComponentReplHB:
		return "REPL_HB"
	case ComponentRollback:
		return "ROLLBACK"
	case ComponentSharding:
		return "SHARDING"
	case ComponentShardingRefr:
		return "SH_REFR"
	case ComponentStorage:
		return "STORAGE"
	case ComponentTotal:
		return "TOTAL"
	case ComponentTracking:
		return "TRACKING"
	case ComponentUnknown:
		return "-"
	case ComponentWrite:
		return "WRITE"
	default:
		panic("unrecognized component")
	}
}

type Severity int

const (
	SeverityNone Severity = 0
	SeverityD             = 1 << iota // Debug
	SeverityD1                        // Debug 1
	SeverityD2                        // Debug 2
	SeverityD3                        // Debug 3
	SeverityD4                        // Debug 4
	SeverityD5                        // Debug 5
	SeverityE                         // Error
	SeverityF                         // Severe/Fatal
	SeverityI                         // Information/Log
	SeverityW                         // Warning
)

type Binary uint32

const (
	BinaryAny Binary = iota
	BinaryMongod
	BinaryMongos
)

type Base struct {
	*internal.RuneReader

	Component  Component
	CString    bool
	LineNumber uint
	RawDate    string
	RawContext string
	RawMessage string
	Severity   Severity
}

func NewSeverity(s string) (Severity, bool) {
	switch s {
	case "", "-":
		return SeverityNone, true
	case "D":
		return SeverityD, true
	case "D1":
		return SeverityD1, true
	case "D2":
		return SeverityD2, true
	case "D3":
		return SeverityD3, true
	case "D4":
		return SeverityD4, true
	case "D5":
		return SeverityD5, true
	case "E":
		return SeverityE, true
	case "F":
		return SeverityF, true
	case "I":
		return SeverityI, true
	case "W":
		return SeverityW, true
	default:
		return SeverityNone, false
	}
}

func (s Severity) String() string {
	switch s {
	case SeverityNone:
		return "-"
	case SeverityD:
		return "D"
	case SeverityD1:
		return "D1"
	case SeverityD2:
		return "D2"
	case SeverityD3:
		return "D3"
	case SeverityD4:
		return "D4"
	case SeverityD5:
		return "D5"
	case SeverityE:
		return "E"
	case SeverityF:
		return "F"
	case SeverityI:
		return "I"
	case SeverityW:
		return "W"
	default:
		panic("unrecognized severity")
	}
}

func (b Binary) String() string {
	switch b {
	case BinaryMongod:
		return "mongod"
	case BinaryMongos:
		return "mongos"
	default:
		return "unknown"
	}
}
