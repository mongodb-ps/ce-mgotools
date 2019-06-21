// Package util provides general assistance to the parser and other areas
// of code. The package is entirely self contained and should not reference
// anything not already contained in the standard go release.
package record

import "mgotools/internal"

// The constant arrays that follow should *ALWAYS* be in sorted binary order. A lot of methods expect these arrays
// to be pre-sorted so they can be used with binary searches for fast array comparison. Failure to put these arrays
// in proper binary order will because these methods to fail.

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
	"$nor",
	"$not",
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

// IsContext checks for a bracketed string ([<string>])
func IsContext(value string) bool {
	length := internal.StringLength(value)
	return length > 2 && value[0] == '[' && value[length-1] == ']'
}
