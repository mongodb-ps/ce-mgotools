// A command factory is an easy way of adding new commands without modifying
// the top level code. It uses registration in `init()` methods to add
// commands to a singleton, keeping command setup and initialization coupled
// to the command code.

package command

import (
	"mgotools/internal"

	"github.com/pkg/errors"
)

type Definition struct {
	Usage string
	Flags []Argument
}

type factory struct {
	registry map[string]factoryDefinition
}
type factoryDefinition struct {
	args    Definition
	create  func() (Command, error)
	command Command
}

var instance = &factory{
	registry: make(map[string]factoryDefinition),
}

func GetFactory() *factory {
	return instance
}

func (c *factory) GetNames() []string {
	keys := make([]string, len(c.registry))
	index := 0
	for key := range c.registry {
		keys[index] = key
		index += 1
	}
	return keys
}
func (c *factory) Get(name string) (Command, error) {
	name = internal.StringToLower(name)
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
func (c *factory) GetDefinition(name string) (Definition, bool) {
	name = internal.StringToLower(name)
	reg, ok := c.registry[name]
	if !ok {
		return Definition{}, false
	}
	return reg.args, ok
}
func (c *factory) Register(name string, args Definition, create func() (Command, error)) {
	if name == "" {
		panic("empty name registered in the command factory")
	} else if create == nil {
		panic("command registered without a create method")
	}
	name = internal.StringToLower(name)
	c.registry[name] = factoryDefinition{
		args,
		create,
		nil,
	}
}
