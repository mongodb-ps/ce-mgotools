package command

import (
	"fmt"
	"strings"
)

const (
	Bool Flag = iota
	Int
	IntSourceSlice
	String
	StringSourceSlice
)

type Argument struct {
	Name      string
	ShortName string
	Usage     string
	Type      Flag
}

type ArgumentCollection struct {
	Booleans map[string]bool
	Integers map[string]int
	Strings  map[string]string
}

func MakeCommandArgumentCollection(index int, args map[string]interface{}, cmd Definition) (ArgumentCollection, error) {
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
			case IntSourceSlice:
				values := input.([]int)
				switch {
				case len(values) == 1:
					argsInt[argument.Name] = values[0]
				case index >= len(values):
					return ArgumentCollection{}, fmt.Errorf("--%s must appear for each file", argument.Name)
				default:
					argsInt[argument.Name] = values[index]
				}
			case String:
				argsString[argument.Name] = strings.Join(input.([]string), " ")
			case StringSourceSlice:
				// multiple strings apply to each log individually
				values := input.([]string)
				switch {
				case len(values) == 1:
					argsString[argument.Name] = values[0]
				case index >= len(values):
					return ArgumentCollection{}, fmt.Errorf("--%s must appear for each file", argument.Name)
				default:
					argsString[argument.Name] = values[index]
				}
			}
		}
	}

	return ArgumentCollection{argsBool, argsInt, argsString}, nil
}
