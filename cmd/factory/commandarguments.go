package factory

import (
	"errors"
	"fmt"
	"strings"
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
			case Int:
				argsInt[argument.Name] = input.(int)
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
			case String:
				argsString[argument.Name] = strings.Join(input.([]string), " ")
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
			}
		}
	}
	return CommandArgumentCollection{argsBool, argsInt, argsString}, nil
}
