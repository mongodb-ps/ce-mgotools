//
// mgotools.go
//
// The main utility built with this suite of tools. It takes files as command
// line arguments or stdin and outputs to stdout.
//
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"mgotools/cmd"
	"mgotools/source"
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

func checkClientCommands(context *cli.Context, count int, command cmd.CommandDefinition) error {
	var length = 0
	for _, flag := range command.Flags {
		switch flag.Type {
		case cmd.IntSourceSlice:
			length = len(context.IntSlice(flag.Name))
		case cmd.StringSourceSlice:
			length = len(context.StringSlice(flag.Name))
		}
		if length > count {
			return errors.New("there cannot be more arguments than files")
		}
	}
	return nil
}

func makeClientFlags() []cli.Command {
	var c []cli.Command
	commandFactory := cmd.GetCommandFactory()
	for _, commandName := range commandFactory.GetCommandNames() {
		command, _ := commandFactory.GetCommandDefinition(commandName)
		clientCommand := cli.Command{Name: commandName, Action: runCommand, Usage: command.Usage}
		for _, argument := range command.Flags {
			if argument.ShortName != "" {
				argument.Name = fmt.Sprintf("%s, %s", argument.Name, argument.ShortName)
			}
			switch argument.Type {
			case cmd.Bool:
				clientCommand.Flags = append(clientCommand.Flags, cli.BoolFlag{Name: argument.Name, Usage: argument.Usage})
			case cmd.Int:
				clientCommand.Flags = append(clientCommand.Flags, cli.IntFlag{Name: argument.Name, Usage: argument.Usage})
			case cmd.IntSourceSlice:
				clientCommand.Flags = append(clientCommand.Flags, cli.IntSliceFlag{Name: argument.Name, Usage: argument.Usage})
			case cmd.StringSourceSlice, cmd.String:
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
		commandFactory = cmd.GetCommandFactory()
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

		input := make([]cmd.CommandInput, 0)
		output := cmd.CommandOutput{os.Stdout, os.Stderr}

		// Check for pipe usage.
		pipe, err := os.Stdin.Stat()
		if err != nil {
			panic(err)
		} else if (pipe.Mode() & os.ModeNamedPipe) != 0 {
			if argc > 0 {
				return errors.New("file arguments and input pipes cannot be used simultaneously")
			}

			// Add stdin to the list of input files.
			args, err := cmd.MakeCommandArgumentCollection(0, getArgumentMap(cmdDefinition, c), cmdDefinition)
			if err != nil {
				return err
			}
			fileCount = 1
			input = append(input, cmd.CommandInput{
				Arguments: args,
				Name:      "stdin",
				Length:    int64(0),
				Reader:    source.NewAccumulator(os.Stdin),
			})
		}

		// Loop through each argument and add files to the command.
		for index := 0; index < argc; index += 1 {
			path := clientContext.Get(index)
			size := int64(0)

			if s, err := os.Stat(path); os.IsNotExist(err) {
				util.Debug("%s skipped (%s)", path, err)
				continue
			} else {
				size = s.Size()
			}

			// Open the file and check for errors.
			file, err := os.OpenFile(path, os.O_RDONLY, 0)
			if err != nil {
				return err
			}

			args, err := cmd.MakeCommandArgumentCollection(index, getArgumentMap(cmdDefinition, c), cmdDefinition)
			if err != nil {
				return err
			}

			fileCount += 1
			input = append(input, cmd.CommandInput{
				Arguments: args,
				Name:      filepath.Base(path),
				Length:    size,
				Reader:    source.NewAccumulator(file),
			})
		}

		// Check for basic command sanity.
		if err := checkClientCommands(c, fileCount, cmdDefinition); err != nil {
			return err
		}

		// Run the actual command.
		if err := cmd.RunCommand(command, input, output); err != nil {
			return err
		}

		//util.Debug("Finished at %s (%s)", time.Now(), time.Since(start).String())
		return nil
	}
}

func getArgumentMap(commandDefinition cmd.CommandDefinition, c *cli.Context) map[string]interface{} {
	out := make(map[string]interface{})
	for _, arg := range commandDefinition.Flags {
		if c.IsSet(arg.Name) {
			switch arg.Type {
			case cmd.Bool:
				out[arg.Name] = c.Bool(arg.Name)
			case cmd.Int:
				out[arg.Name] = c.Int(arg.Name)
			case cmd.IntSourceSlice:
				out[arg.Name] = c.IntSlice(arg.Name)
			case cmd.String, cmd.StringSourceSlice:
				out[arg.Name] = c.StringSlice(arg.Name)
			}
		}
	}
	return out
}
