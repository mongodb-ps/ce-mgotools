// Package util provides general assistance to the parser and other areas
// of code. The package is entirely self contained and should not reference
// anything not already contained in the standard go release.
package mongo

import "mgotools/util"

type Object = map[string]interface{}
type Array = []interface{}

// The constant arrays that follow should *ALWAYS* be in sorted binary order. A lot of methods expect these arrays
// to be pre-sorted so they can be used with binary searches for fast array comparison. Failure to put these arrays
// in proper binary order will because these methods to fail.

// The components array contains all component strings.
var COMPONENTS = []string{
	"ACCESS",        // 3.0, 3.2, 3.4, 3.6
	"ACCESSCONTROL", // 3.0, 3.2, 3.4, 3.6
	"ASIO",          // 3.2, 3.4, 3.6
	"BRIDGE",        // 3.0, 3.2, 3.4, 3.6
	"COMMAND",       // 3.0, 3.2, 3.4, 3.6
	"CONTROL",       // 3.0, 3.2, 3.4, 3.6
	"DEFAULT",       // 3.0, 3.2, 3.4, 3.6
	"EXECUTOR",      // 3.2, 3.4, 3.6
	"FTDC",          // 3.2, 3.4, 3.6
	"GEO",           // 3.0, 3.2, 3.4, 3.6
	"HEARTBEATS",    // 3.6
	"INDEX",         // 3.0, 3.2, 3.4, 3.6
	"JOURNAL",       // 3.0, 3.2, 3.4, 3.6
	"NETWORK",       // 3.0, 3.2, 3.4, 3.6
	"QUERY",         // 3.0, 3.2, 3.4, 3.6
	"REPL",          // 3.0, 3.2, 3.4, 3.6
	"REPL_HB",       // 3.6
	"REPLICATION",   // 3.0, 3.2, 3.4, 3.6
	"ROLLBACK",      // 3.6
	"SHARDING",      // 3.0, 3.2, 3.4, 3.6
	"STORAGE",       // 3.0, 3.2, 3.4, 3.6
	"TOTAL",         // 3.0, 3.2, 3.4, 3.6
	"TRACKING",      // 3.4, 3.6
	"WRITE",         // 3.0, 3.2, 3.4, 3.6
	"-",
}

// ref: /mongo/src/mongo/db/curop.cpp
var COUNTERS = map[string]string{
	"cursorExhausted":  "cursorExhausted",
	"cursorid":         "cursorid",
	"docsExamined":     "docsExamined",
	"fastmod":          "fastmod",
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
	"replanned":        "replanned",
	"reslen":           "reslen",
	"scanAndOrder":     "scanAndOrder",
	"upsert":           "upsert",
	"writeConflicts":   "writeConflicts",
}

var OPERATIONS = []string{
	"aggregate",
	"command",
	"count",
	"distinct",
	"find",
	"geoNear",
	"geonear",
	"getMore",
	"getmore",
	"insert",
	"mapreduce",
	"query",
	"remove",
	"update",
}

var OPERATORS_COMPARISON = []string{
	"$all",
	"$bitsAllClear", // 3.2
	"$bitsAllSet",   // 3.2
	"$bitsAnyClear", // 3.2
	"$bitsAnySet",   // 3.2
	"$eq",           // 3.0
	"$exists",
	"$gt",
	"$gte",
	"$in",
	"$lt",
	"$lte",
	"$ne",
	"$size",
	"$type",
}

var OPERATORS_LOGICAL = []string{
	"$and",
	"$nin",
	"$not",
	"$nor",
	"$or",
}

var OPERATORS_EXPRESSION = []string{
	"$box",          // $geoWithin
	"$center",       // $geoWithin
	"$centerSphere", // $geoWithin
	"$comment",
	"$elemMatch",
	"$expr",
	"$geoIntersects", // 2.4
	"$geoWithin",     // 2.4
	"$geometry",      // $geoIntersects, $geoWithin
	"$jsonSchema",    // 3.6
	"$mod",
	"$near",
	"$nearSphere",
	"$regex",
	"$text",
	"$where",
}

// The severities variable contains all severities encountered in MongoDB.
var SEVERITIES = []string{"D", "E", "F", "I", "W"}

// IsComponent checks a string value against the possible components array.
func IsComponent(value string) bool {
	return util.ArrayMatchString(COMPONENTS, value)
}

// IsContext checks for a bracketed string ([<string>])
func IsContext(value string) bool {
	length := util.StringLength(value)
	return length > 2 && value[0] == '[' && value[length-1] == ']'
}

// IsSeverity checks a string value against the severities array.
func IsSeverity(value string) bool {
	return util.StringLength(value) == 1 && util.ArrayMatchString(SEVERITIES, value)
}
