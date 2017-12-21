// Package util provides general assistance to the parser and other areas
// of code. The package is entirely self contained and should not reference
// anything not already contained in the standard go release.
package mongo

// The components array contains all component strings.
var COMPONENTS = []string{
	"ACCESS",        // 3.0 ?
	"ACCESSCONTROL", // 3.2 ?
	"ASIO",          // 3.4 ?
	"BRIDGE",        // 3.4 ?
	"COMMAND",       // 3.x
	"CONTROL",
	"DEFAULT",
	"EXECUTOR",
	"FTDC", // 3.4 ?
	"GEO",
	"HEARTBEATS", // 3.2 ?
	"INDEX",
	"JOURNAL",
	"NETWORK",
	"QUERY",
	"REPL",        // 3.0 ?
	"REPLICATION", // 3.2 ?
	"SHARDING",
	"STORAGE",
	"TOTAL",
	"TRACKING", // 3.4 ?
	"WRITE",
	"-",
}

// ref: /mongo/src/mongo/db/curop.cpp
var COUNTERS = map[string]string{
	"cursorExhausted":  "cursorExhausted",
	"cursorid":         "cursorid",
	"docsExamined":     "docsExamined",
	"fastmodinsert":    "fastmodinsert",
	"exhaust":          "exhaust",
	"fromMultiPlanner": "fromMultiPlanner",
	"hasSortStage":     "hasSortStage",
	"idhack":           "idhack",
	"keysDeleted":      "keysDeleted",
	"keysExamined":     "keysExamined",
	"keysInserted":     "keysInserted",
	"ndeleted":         "ndeleted",
	"nDeleted":         "ndeleted",
	"ninserted":        "ninserted",
	"nInserted":        "ninserted",
	"nmatched":         "nmatched",
	"nMatched":         "nmatched",
	"nmodified":        "nmodified",
	"nModified":        "nmodified",
	"nmoved":           "nmoved",
	"nscanned":         "keysExamined",
	"nscannedObjects":  "docsExamined",
	"nreturned":        "nreturned",
	"ntoreturn":        "ntoreturn",
	"ntoskip":          "notoskip",
	"planSummary":      "planSummary",
	"numYields":        "numYields",
	"keyUpdates":       "keyUpdates",
	"r":                "r",
	"R":                "r",
	"replanned":        "replanned",
	"reslen":           "reslen",
	"scanAndOrder":     "scanAndOrder",
	"upsert":           "upsert",
	"w":                "w",
	"W":                "w",
	"writeConflicts":   "writeConflicts",
}

var OPERATIONS = []string{"command", "getmore", "insert", "query", "remove", "update"}

// The severities variable contains all severities encountered in MongoDB.
var SEVERITIES = []string{"D", "E", "F", "I", "W"}
