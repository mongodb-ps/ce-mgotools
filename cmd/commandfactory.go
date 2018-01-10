package cmd

// This file exists to make adding new commands easy. Simply add a command to the factory, ensure it implements
// the command interface, and like magic, a command is born!
type CommandDefinition struct {
	Usage string
	Flags []CommandArgument
}

func GetCommandNames() []string {
	return []string{
		"filter",
	}
}

// A general purpose method to create different types of command structures.
func CommandFactory(command string, options BaseOptions) Command {
	switch command {
	case "filter":
		return &filterCommand{}

	default:
		panic("unexpected command received")
	}
}

func GetCommandDefinition(command string) (CommandDefinition, bool) {
	switch command {
	case "filter":
		return CommandDefinition{
			Usage: "filters a log file",
			Flags: []CommandArgument{
				{Name: "command", Type: String, Usage: "only output log lines which are `COMMAND` of a given type. Examples: \"distinct\", \"isMaster\", \"replSetGetStatus\""},
				{Name: "component", ShortName: "c", Type: String, Usage: "find all lines matching `COMPONENT`"},
				{Name: "context", Type: StringFileSlice, Usage: "find all lines matching `CONTEXT`"},
				{Name: "connection", ShortName: "x", Type: Int, Usage: "find all lines identified as part of `CONNECTION`"},
				{Name: "exclude", Type: Bool, Usage: "exclude matching lines rather than including them"},
				{Name: "fast", Type: Int, Usage: "returns only operations faster than `FAST` milliseconds"},
				{Name: "from", ShortName: "f", Type: StringFileSlice, Usage: "ignore all entries before `DATE` (see help for date formatting)"},
				{Name: "message", Type: Bool, Usage: "excludes all non-message portions of each line"},
				{Name: "namespace", Type: String, Usage: "filter by `NAMESPACE` so only lines matching the namespace will be returned"},
				{Name: "marker", Type: StringFileSlice, Usage: "append a pre-defined marker (filename, enum, alpha, none) or custom marker (one per file) identifying the source file of each line"},
				{Name: "severity", ShortName: "i", Type: String, Usage: "find all lines of `SEVERITY`"},
				{Name: "shorten", Type: Int, Usage: "reduces output by truncating log lines to `LENGTH` characters"},
				{Name: "slow", Type: Int, Usage: "returns only operations slower than `SLOW` milliseconds"},
				{Name: "timezone", Type: IntFileSlice, Usage: "timezone adjustment: add `N` minutes to the corresponding log file"},
				{Name: "to", ShortName: "t", Type: StringFileSlice, Usage: "ignore all entries after `DATE` (see help for date formatting)"},
				{Name: "word", Type: StringFileSlice, Usage: "only output lines matching `WORD`"},
			},
		}, true
	default:
		return CommandDefinition{}, false
	}
}
