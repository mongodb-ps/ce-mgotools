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

	"mgotools/command"
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

func checkClientCommands(context *cli.Context, count int, def command.Definition) error {
	var length = 0
	for _, flag := range def.Flags {
		switch flag.Type {
		case command.IntSourceSlice:
			length = len(context.IntSlice(flag.Name))
		case command.StringSourceSlice:
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
	commandFactory := command.GetFactory()
	for _, commandName := range commandFactory.GetNames() {
		cmd, _ := commandFactory.GetDefinition(commandName)
		clientCommand := cli.Command{Name: commandName, Action: runCommand, Usage: cmd.Usage}
		for _, argument := range cmd.Flags {
			if argument.ShortName != "" {
				argument.Name = fmt.Sprintf("%s, %s", argument.Name, argument.ShortName)
			}
			switch argument.Type {
			case command.Bool:
				clientCommand.Flags = append(clientCommand.Flags, cli.BoolFlag{Name: argument.Name, Usage: argument.Usage})
			case command.Int:
				clientCommand.Flags = append(clientCommand.Flags, cli.IntFlag{Name: argument.Name, Usage: argument.Usage})
			case command.IntSourceSlice:
				clientCommand.Flags = append(clientCommand.Flags, cli.IntSliceFlag{Name: argument.Name, Usage: argument.Usage})
			case command.StringSourceSlice, command.String:
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
		commandFactory = command.GetFactory()
		clientContext  = c.Args()
		//start          = time.Now()
	)
	if c.Command.Name == "" {
		return errors.New("command required")
	} else if cmdDefinition, ok := commandFactory.GetDefinition(c.Command.Name); !ok {
		return fmt.Errorf("unrecognized command %s", c.Command.Name)
	} else {
		//util.Debug("Command: %s, starting: %s", c.Command.Name, time.Now())

		cmd, err := commandFactory.Get(c.Command.Name)
		if err != nil {
			return err
		}

		// Get argument count.
		argc := c.NArg()
		fileCount := 0

		input := make([]command.Input, 0)
		output := command.Output{os.Stdout, os.Stderr}

		// Check for pipe usage.
		pipe, err := os.Stdin.Stat()
		if err != nil {
			panic(err)
		} else if (pipe.Mode() & os.ModeNamedPipe) != 0 {
			if argc > 0 {
				return errors.New("file arguments and input pipes cannot be used simultaneously")
			}

			// Add stdin to the list of input files.
			args, err := command.MakeCommandArgumentCollection(0, getArgumentMap(cmdDefinition, c), cmdDefinition)
			if err != nil {
				return err
			}

			fileCount = 1
			stdio, err := source.NewLog(os.Stdin)

			input = append(input, command.Input{
				Arguments: args,
				Name:      "stdin",
				Length:    int64(0),
				Reader:    source.NewAccumulator(stdio),
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

			args, err := command.MakeCommandArgumentCollection(index, getArgumentMap(cmdDefinition, c), cmdDefinition)
			if err != nil {
				return err
			}

			logfile, err := source.NewLog(file)
			if err != nil {
				return err
			}

			fileCount += 1
			input = append(input, command.Input{
				Arguments: args,
				Name:      filepath.Base(path),
				Length:    size,
				Reader:    source.NewAccumulator(logfile),
			})
		}

		// Check for basic command sanity.
		if err := checkClientCommands(c, fileCount, cmdDefinition); err != nil {
			return err
		}

		// Run the actual command.
		if err := command.RunCommand(cmd, input, output); err != nil {
			return err
		}

		//util.Debug("Finished at %s (%s)", time.Now(), time.Since(start).String())
		return nil
	}
}

func getArgumentMap(commandDefinition command.Definition, c *cli.Context) map[string]interface{} {
	out := make(map[string]interface{})
	for _, arg := range commandDefinition.Flags {
		if c.IsSet(arg.Name) {
			switch arg.Type {
			case command.Bool:
				out[arg.Name] = c.Bool(arg.Name)
			case command.Int:
				out[arg.Name] = c.Int(arg.Name)
			case command.IntSourceSlice:
				out[arg.Name] = c.IntSlice(arg.Name)
			case command.String, command.StringSourceSlice:
				out[arg.Name] = c.StringSlice(arg.Name)
			}
		}
	}
	return out
}
