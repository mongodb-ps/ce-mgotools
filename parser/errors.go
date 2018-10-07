package parser

/*
 * Log parser errors
 */
type VersionDateUnmatched struct{}
type VersionMessageUnmatched struct{}

type ErrorVersionUnmatched struct {
	Message string
}

func (e VersionDateUnmatched) Error() string {
	return "unmatched date string"
}
func (e VersionMessageUnmatched) Error() string {
	return "unmatched or empty message string"
}
func (e ErrorVersionUnmatched) Error() string {
	if e.Message != "" {
		return "Log message not recognized: " + e.Message
	} else {
		return "Log message not recognized"
	}
}
