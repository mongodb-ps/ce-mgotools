package errors

import "errors"

/*
 * General purpose formatting errors.
 */
var CommandNotFound = errors.New("command not found")
var CommandStructure = errors.New("command structure unexpected")
var ComponentUnmatched = errors.New("component unmatched")
var ControlUnrecognized = VersionUnmatched{Message: "unrecognized control message"}
var CounterUnrecognized = VersionUnmatched{Message: "unrecognized counter"}
var MetadataUnmatched = VersionUnmatched{"unexpected connection meta format"}
var NetworkUnrecognized = VersionUnmatched{"unrecognized network message"}
var NoNamespaceFound = errors.New("no namespace found")
var NoPlanSummaryFound = errors.New("no plan summary found")
var NoStartupArgumentsFound = errors.New("no startup arguments found")
var OperationStructure = errors.New("operation structure unexpected")
var StorageUnmatched = VersionUnmatched{"unrecognized storage option"}
var UnexpectedExceptionFormat = errors.New("error parsing exception")
var UnexpectedVersionFormat = errors.New("unexpected version format")

/*
 * Log parser errors
 */
var VersionDateUnmatched = errors.New("unmatched date string")
var VersionMessageUnmatched = errors.New("unmatched or empty message string")

type VersionUnmatched struct {
	Message string
}

func (e VersionUnmatched) Error() string {
	if e.Message != "" {
		return "Log message not recognized: " + e.Message
	} else {
		return "Log message not recognized"
	}
}
