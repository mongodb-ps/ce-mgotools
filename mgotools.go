package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "mgotools/cmd"
	"mgotools/cmd/factory"
	"mgotools/util"

	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()

	app.Name = "mgotools"
	app.Description = "A collection of tools designed to help parse and understand MongoDB logs"
	app.Action = runCommand

	app.Commands = makeClientFlags()

	app.Flags = []cli.Flag{
		//cli.BoolFlag{Name: "linear, e", Usage: "parse input files linearly in order they are supplied (disable concurrency)"},
		cli.BoolFlag{Name: "verbose, v", Usage: "outputs additional information about the parser"},
	}
	cli.VersionFlag = cli.BoolFlag{Name: "version, V"}
	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}

func checkClientCommands(context *cli.Context, count int, command factory.CommandDefinition) error {
	var length = 0
	for _, flag := range command.Flags {
		switch flag.Type {
		case factory.IntFileSlice:
			length = len(context.IntSlice(flag.Name))
		case factory.StringFileSlice:
			length = len(context.StringSlice(flag.Name))
		}
		if length > count {
			return errors.New("there cannot be more arguments than files")
		}
	}
	return nil
}

func makeClientFlags() []cli.Command {
	c := []cli.Command{}
	commandFactory := factory.GetCommandFactory()
	for _, commandName := range commandFactory.GetCommandNames() {
		command, _ := commandFactory.GetCommandDefinition(commandName)
		clientCommand := cli.Command{Name: commandName, Action: runCommand, Usage: command.Usage}
		for _, argument := range command.Flags {
			if argument.ShortName != "" {
				argument.Name = fmt.Sprintf("%s, %s", argument.Name, argument.ShortName)
			}
			switch argument.Type {
			case factory.Bool:
				clientCommand.Flags = append(clientCommand.Flags, cli.BoolFlag{Name: argument.Name, Usage: argument.Usage})
			case factory.Int:
				clientCommand.Flags = append(clientCommand.Flags, cli.IntFlag{Name: argument.Name, Usage: argument.Usage})
			case factory.IntFileSlice:
				clientCommand.Flags = append(clientCommand.Flags, cli.IntSliceFlag{Name: argument.Name, Usage: argument.Usage})
			case factory.StringFileSlice, factory.String:
				clientCommand.Flags = append(clientCommand.Flags, cli.StringSliceFlag{Name: argument.Name, Usage: argument.Usage})
			}
		}
		c = append(c, clientCommand)
	}
	return c
}

func runCommand(c *cli.Context) error {
	// Pull arguments from the helper interpreter.
	var (
		commandFactory = factory.GetCommandFactory()
		clientContext  = c.Args()
		//start          = time.Now()
	)
	if c.Command.Name == "" {
		return errors.New("command required")
	} else if cmdDefinition, ok := commandFactory.GetCommandDefinition(c.Command.Name); !ok {
		return fmt.Errorf("unrecognized command %s", c.Command.Name)
	} else {
		//util.Debug("Command: %s, starting: %s", c.Command.Name, time.Now())
		command, err := commandFactory.GetCommand(c.Command.Name)
		if err != nil {
			return err
		}
		// Get argument count.
		argc := c.NArg()
		fileCount := 0
		input := factory.NewInputHandler()
		output := factory.NewOutputHandler(os.Stdout, os.Stderr)
		// Check for pipe usage.
		pipe, err := os.Stdin.Stat()
		if err != nil {
			panic(err)
		} else if (pipe.Mode() & os.ModeNamedPipe) != 0 {
			if argc > 0 {
				return errors.New("file arguments and input pipes cannot be used simultaneously")
			}

			// Add stdin to the list of input files.
			args, err := factory.MakeCommandArgumentCollection(0, getArgumentMap(cmdDefinition, c), cmdDefinition)
			if err != nil {
				return err
			}
			fileCount = 1
			input.AddHandle("stdin", os.Stdin, args)
		}
		// Loop through each argument and add files to the command.
		for index := 0; index < argc; index += 1 {
			path := clientContext.Get(index)
			if _, err := os.Stat(path); os.IsNotExist(err) {
				util.Debug("%s skipped (%s)", path, err)
				continue
			}
			// Open the file and check for errors.
			file, err := os.OpenFile(path, os.O_RDONLY, 0)
			if err != nil {
				return err
			}
			args, err := factory.MakeCommandArgumentCollection(index, getArgumentMap(cmdDefinition, c), cmdDefinition)
			if err != nil {
				return err
			}
			fileCount += 1
			input.AddHandle(filepath.Base(path), file, args)
		}
		// Check for basic command sanity.
		if err := checkClientCommands(c, fileCount, cmdDefinition); err != nil {
			return err
		}
		// Run the actual command.
		if err = factory.RunCommand(command, input, output); err != nil {
			fmt.Println(fmt.Sprintf("Error: %s", err))
		}
		//util.Debug("Finished at %s (%s)", time.Now(), time.Since(start).String())
		return err
	}
}
func getArgumentMap(commandDefinition factory.CommandDefinition, c *cli.Context) map[string]interface{} {
	out := make(map[string]interface{})
	for _, arg := range commandDefinition.Flags {
		if c.IsSet(arg.Name) {
			switch arg.Type {
			case factory.Bool:
				out[arg.Name] = c.Bool(arg.Name)
			case factory.Int:
				out[arg.Name] = c.Int(arg.Name)
			case factory.IntFileSlice:
				out[arg.Name] = c.IntSlice(arg.Name)
			case factory.String, factory.StringFileSlice:
				out[arg.Name] = c.StringSlice(arg.Name)
			}
		}
	}
	return out
}
