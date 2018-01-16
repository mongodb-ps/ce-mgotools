// Package util provides general assistance to the parser and other areas
// of code. The package is entirely self contained and should not reference
// anything not already contained in the standard go release.
package mongo

type Object map[string]interface{}
type Array []interface{}

// The constant arrays that follow should *ALWAYS* be in sorted binary order. A lot of methods expect these arrays
// to be pre-sorted so they can be used with binary searches for fast array comparison. Failure to put these arrays
// in proper binary order will because these methods to fail.

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
	"$nin",
	"$size",
	"$type",
}

var OPERATORS_LOGICAL = []string{
	"$and",
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
