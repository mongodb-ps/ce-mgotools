package cmd

import (
	"fmt"
)

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

func GetCommandDefinition(command string) CommandDefinition {
	switch command {
	case "filter":
		return CommandDefinition{
			Usage: "filters a log file",
			Flags: []CommandArgument{
				{Name: "component", ShortName: "c", Type: String, Usage: "find all lines matching `COMPONENT`"},
				{Name: "context", Type: StringSlice, Usage: "find all lines matching `CONTEXT`"},
				{Name: "connection", ShortName: "x", Type: Int, Usage: "find all lines identified as part of `CONNECTION`"},
				{Name: "exclude", Type: Bool, Usage: "exclude matching lines rather than including them"},
				{Name: "from", ShortName: "f", Type: StringSlice, Usage: "ignore all entries before `DATE` (see help for date formatting)"},
				{Name: "message", Type: Bool, Usage: "excludes all non-message portions of each line"},
				{Name: "severity", ShortName: "i", Type: String, Usage: "find all lines of `SEVERITY`"},
				{Name: "shorten", Type: Int, Usage: "reduces output by truncating log lines to `LENGTH` characters"},
				{Name: "timezone", Type: IntSlice, Usage: "timezone adjustment: add `N` minutes to the corresponding log file"},
				{Name: "to", ShortName: "t", Type: StringSlice, Usage: "ignore all entries after `DATE` (see help for date formatting)"},
			},
		}

	default:
		panic(fmt.Sprintf("unexpected command %s received", command))
	}
}
