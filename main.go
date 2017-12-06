package main

import (
	"github.com/urfave/cli"
	mgocommands "mgotools/cmd"
	"mgotools/util"

	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	app := cli.NewApp()

	app.Name = "mgotools"
	app.Description = "A collection of tools designed to help parse and understand MongoDB logs"
	app.Action = runCommand

	app.Commands = makeCommands()

	app.Flags = []cli.Flag{
		//cli.BoolFlag{Name: "linear, e", Usage: "parse input files linearly in order they are supplied (disable concurrency)"},
		cli.BoolFlag{Name: "verbose, v", Usage: "outputs additional information about the parser"},
	}
	cli.VersionFlag = cli.BoolFlag{Name: "version, V"}
	if err := app.Run(os.Args); err != nil {
		fmt.Println(err)
	}
}

func checkCommands(context *cli.Context, count int, command mgocommands.CommandDefinition) error {
	var length int = 0
	for _, flag := range command.Flags {
		switch flag.Type {
		case mgocommands.IntSlice:
			length = len(context.IntSlice(flag.Name))
		case mgocommands.StringSlice:
			length = len(context.StringSlice(flag.Name))
		}
		if length > count {
			return errors.New("there cannot be more arguments than files")
		}
	}
	return nil
}

func makeCommands() []cli.Command {
	c := []cli.Command{}
	for _, commandName := range mgocommands.GetCommandNames() {
		command := mgocommands.GetCommandDefinition(commandName)
		clientCommand := cli.Command{Name: commandName, Action: runCommand, Usage: command.Usage}

		for _, argument := range command.Flags {
			if argument.ShortName != "" {
				argument.Name = fmt.Sprintf("%s, %s", argument.Name, argument.ShortName)
			}
			switch argument.Type {
			case mgocommands.Bool:
				clientCommand.Flags = append(clientCommand.Flags, cli.BoolFlag{Name: argument.Name, Usage: argument.Usage})
			case mgocommands.Int:
				clientCommand.Flags = append(clientCommand.Flags, cli.IntFlag{Name: argument.Name, Usage: argument.Usage})
			case mgocommands.IntSlice:
				clientCommand.Flags = append(clientCommand.Flags, cli.IntSliceFlag{Name: argument.Name, Usage: argument.Usage})
			case mgocommands.StringSlice, mgocommands.String:
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
		args          mgocommands.CommandArgumentCollection
		clientContext cli.Args  = c.Args()
		start         time.Time = time.Now()
	)

	util.Debug("Command: %s, starting: %s", c.Command.Name, time.Now())

	cmdDefinition := mgocommands.GetCommandDefinition(c.Command.Name)
	command := mgocommands.CommandFactory(c.Command.Name, makeOptions(c, cmdDefinition))

	// Get argument count.
	argc := c.NArg()
	fileCount := 0
	input := mgocommands.NewInputHandler()
	output := mgocommands.NewOutputHandler(os.Stdout, os.Stderr)

	// Check for pipe usage.
	pipe, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	} else if (pipe.Mode() & os.ModeNamedPipe) != 0 {
		if argc > 0 {
			return errors.New("file arguments and input pipes cannot be used simultaneously")
		}

		// Add stdin to the list of input files.
		if args, err = makeContext(0, c, cmdDefinition); err != nil {
			return err
		}
		fileCount = 1
		input.AddHandle(os.Stdin, args)
	}
	// Loop through each argument and add files to the command.
	for i := 0; i < argc; i += 1 {
		path := clientContext.Get(i)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			util.Debug("%s skipped (%s)", path, err)
			continue
		}
		// Open the file and check for errors.
		file, err := os.OpenFile(path, os.O_RDONLY, 0)

		if err != nil {
			return err
		}

		if args, err = makeContext(i, c, cmdDefinition); err != nil {
			return err
		}
		fileCount += 1
		input.AddHandle(file, args)
	}
	// Check for basic command sanity.
	if err := checkCommands(c, fileCount, cmdDefinition); err != nil {
		return err
	}
	// Run the actual command.
	if err = mgocommands.RunCommand(command, args, input, output); err != nil {
		fmt.Println(fmt.Sprintf("Error: %s", err))
	}
	util.Debug("Finished at %s (%s)", time.Now(), time.Since(start).String())
	return err
}

func makeContext(index int, c *cli.Context, cmd mgocommands.CommandDefinition) (mgocommands.CommandArgumentCollection, error) {
	var (
		argsBool   map[string]bool   = make(map[string]bool)
		argsInt    map[string]int    = make(map[string]int)
		argsString map[string]string = make(map[string]string)
	)
	for _, argument := range cmd.Flags {
		if c.IsSet(argument.Name) {
			switch argument.Type {
			case mgocommands.Bool:
				argsBool[argument.Name] = c.Bool(argument.Name)
				fmt.Println(fmt.Sprintf("** Bool: %s = %v", argument.Name, argsBool[argument.Name]))
			case mgocommands.Int:
				argsInt[argument.Name] = c.Int(argument.Name)
				fmt.Println(fmt.Sprintf("** Int: %s = %s", argument.Name, argsInt[argument.Name]))
			case mgocommands.IntSlice:
				values := c.IntSlice(argument.Name)
				switch {
				case len(values) == 1:
					argsInt[argument.Name] = values[0]
				case index >= len(values):
					return mgocommands.CommandArgumentCollection{}, errors.New(fmt.Sprintf("--%s must appear for each file", argument.Name))
				default:
					argsInt[argument.Name] = values[index]
				}
				fmt.Println(fmt.Sprintf("** IntSlice: %s = %d", argument.Name, argsInt[argument.Name]))
			case mgocommands.String:
				argsString[argument.Name] = strings.Join(c.StringSlice(argument.Name), " ")
				fmt.Println(fmt.Sprintf("** String: %s = %s", argument.Name, argsString[argument.Name]))
			case mgocommands.StringSlice:
				// multiple strings apply to each log individually
				values := c.StringSlice(argument.Name)
				switch {
				case len(values) == 1:
					argsString[argument.Name] = values[0]
				case index >= len(values):
					return mgocommands.CommandArgumentCollection{}, errors.New(fmt.Sprintf("--%s must appear for each file", argument.Name))
				default:
					argsString[argument.Name] = values[index]
				}
				fmt.Println(fmt.Sprintf("** StringSlice: %s = %s", argument.Name, argsString[argument.Name]))
			}
		}
	}
	return mgocommands.CommandArgumentCollection{argsBool, argsInt, argsString}, nil
}

func makeOptions(c *cli.Context, cmd mgocommands.CommandDefinition) mgocommands.BaseOptions {
	options := mgocommands.BaseOptions{
		DateFormat:  "2006-01-02T15:04:05.000-0700",
		LinearParse: c.Bool("linear"),
		Verbose:     c.Bool("verbose"),
	}
	return options
}
