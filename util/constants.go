// Package util provides general assistance to the parser and other areas
// of code. The package is entirely self contained and should not reference
// anything not already contained in the standard go release.
package util

import "fmt"

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

var COUNTERS = map[string]string{
	"docsExamined":    "docsExamined",
	"idhack":          "idhack",
	"keysExamined":    "keysExamined",
	"ndeleted":        "ndeleted",
	"nDeleted":        "ndeleted",
	"ninserted":       "ninserted",
	"nInserted":       "ninserted",
	"nmatched":        "nmatched",
	"nMatched":        "nmatched",
	"nmodified":       "nmodified",
	"nModified":       "nmodified",
	"nscanned":        "keysExamined",
	"nscannedObjects": "docsExamined",
	"nreturned":       "nreturned",
	"ntoreturn":       "ntoreturn",
	"ntoskip":         "notoskip",
	"planSummary":     "planSummary",
	"numYields":       "numYields",
	"keyUpdates":      "keyUpdates",
	"r":               "r",
	"R":               "r",
	"reslen":          "reslen",
	"scanAndOrder":    "scanAndOrder",
	"w":               "w",
	"W":               "w",
	"writeConflicts":  "writeConflicts",
}

var OPERATIONS = []string{"command", "getmore", "insert", "query", "remove", "update"}

// The severities variable contains all severities encountered in MongoDB.
var SEVERITIES = []string{"D", "E", "F", "I", "W"}

func Debug(format string, v ...interface{}) {
	fmt.Println(fmt.Sprintf(format, v...))
}

// IsComponent checks a string value against the possible components array.
func IsComponent(value string) bool {
	return ArrayMatchString(COMPONENTS, value)
}

// IsContext checks for a bracketed string ([<string>])
func IsContext(value string) bool {
	length := StringLength(value)
	return length > 2 && value[0] == '[' && value[length-1] == ']'
}

// IsSeverity checks a string value against the severities array.
func IsSeverity(value string) bool {
	return StringLength(value) == 1 && ArrayMatchString(SEVERITIES, value)
}
