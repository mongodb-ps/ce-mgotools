package parser

/*
 * Log parser errors
 */
type LogVersionDateUnmatched struct{}
type LogVersionMessageUnmatched struct{}

type LogVersionErrorUnmatched struct {
	Message string
}

func (e LogVersionDateUnmatched) Error() string {
	return "unmatched date string"
}
func (e LogVersionMessageUnmatched) Error() string {
	return "unmatched or empty message string"
}
