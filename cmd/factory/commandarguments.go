package factory

import (
	"errors"
	"fmt"
	"strings"

	"mgotools/util"
)

const (
	Bool CommandFlag = iota
	Int
	IntFileSlice
	String
	StringFileSlice
)

type CommandArgument struct {
	Name      string
	ShortName string
	Usage     string
	Type      CommandFlag
}

type CommandArgumentCollection struct {
	Booleans map[string]bool
	Integers map[string]int
	Strings  map[string]string
}

func MakeCommandArgumentCollection(index int, args map[string]interface{}, cmd CommandDefinition) (CommandArgumentCollection, error) {
	var (
		argsBool   = make(map[string]bool)
		argsInt    = make(map[string]int)
		argsString = make(map[string]string)
	)
	for _, argument := range cmd.Flags {
		input, ok := args[argument.Name]
		if ok {
			switch argument.Type {
			case Bool:
				argsBool[argument.Name] = input.(bool)
				util.Debug("** Bool: %s = %v", argument.Name, argsBool[argument.Name])
			case Int:
				argsInt[argument.Name] = input.(int)
				util.Debug("** Int: %s = %s", argument.Name, argsInt[argument.Name])
			case IntFileSlice:
				values := input.([]int)
				switch {
				case len(values) == 1:
					argsInt[argument.Name] = values[0]
				case index >= len(values):
					return CommandArgumentCollection{}, errors.New(fmt.Sprintf("--%s must appear for each file", argument.Name))
				default:
					argsInt[argument.Name] = values[index]
				}
				util.Debug("** IntFileSlice: %s = %d", argument.Name, argsInt[argument.Name])
			case String:
				argsString[argument.Name] = strings.Join(input.([]string), " ")
				util.Debug("** String: %s = %s", argument.Name, argsString[argument.Name])
			case StringFileSlice:
				// multiple strings apply to each log individually
				values := input.([]string)
				switch {
				case len(values) == 1:
					argsString[argument.Name] = values[0]
				case index >= len(values):
					return CommandArgumentCollection{}, errors.New(fmt.Sprintf("--%s must appear for each file", argument.Name))
				default:
					argsString[argument.Name] = values[index]
				}
				util.Debug("** StringFileSlice: %s = %s", argument.Name, argsString[argument.Name])
			}
		}
	}
	return CommandArgumentCollection{argsBool, argsInt, argsString}, nil
}
