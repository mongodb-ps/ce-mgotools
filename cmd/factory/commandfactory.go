package factory

import (
	"mgotools/util"

	"github.com/pkg/errors"
)

// This file exists to make adding new commands easy. Simply add a command to the factory, ensure it implements
// the command interface, and like magic, a command is born!
type CommandDefinition struct {
	Usage string
	Flags []CommandArgument
}
type commandFactory struct {
	registry map[string]commandFactoryDefinition
}
type commandFactoryDefinition struct {
	args    CommandDefinition
	create  func() (Command, error)
	command Command
}

var factory = &commandFactory{
	registry: make(map[string]commandFactoryDefinition),
}

func GetCommandFactory() *commandFactory {
	return factory
}

func (c *commandFactory) GetCommandNames() []string {
	keys := make([]string, len(c.registry))
	index := 0
	for key := range c.registry {
		keys[index] = key
		index += 1
	}
	return keys
}
func (c *commandFactory) GetCommand(name string) (Command, error) {
	name = util.StringToLower(name)
	reg, ok := c.registry[name]
	if !ok {
		return nil, errors.New("command not registered")
	}
	if reg.command == nil {
		cmd, err := reg.create()
		if err != nil {
			return nil, err
		}
		reg.command = cmd
	}
	return reg.command, nil
}
func (c *commandFactory) GetCommandDefinition(name string) (CommandDefinition, bool) {
	name = util.StringToLower(name)
	reg, ok := c.registry[name]
	if !ok {
		return CommandDefinition{}, false
	}
	return reg.args, ok
}
func (c *commandFactory) Register(name string, args CommandDefinition, create func() (Command, error)) {
	if name == "" {
		panic("empty name registered in the command factory")
	} else if create == nil {
		panic("command registered without a create method")
	}
	name = util.StringToLower(name)
	c.registry[name] = commandFactoryDefinition{
		args,
		create,
		nil,
	}
}
