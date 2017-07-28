package main

import (
	"github.com/urfave/cli"
	mgocommands "mgotools/commands"

	"errors"
	"fmt"
	"os"
	"time"
)

func main() {
	app := cli.NewApp()

	app.Name = "mgotools"
	app.Description = "A collection of tools designed to help parse and understand MongoDB logs"
	app.Action = command

	app.Commands = []cli.Command{
		{
			Name:    "filter",
			Action:  command,
			Aliases: []string{"query", "f"},
			Usage:   "filter a log file",

			Flags: []cli.Flag{
				cli.StringFlag{Name: "pattern", Usage: "only output log lines with a query pattern"},
			},
		},
	}

	app.Flags = []cli.Flag{
		cli.BoolFlag{Name: "verbose, v", Usage: "outputs additional information about the parser"},
	}

	cli.VersionFlag = cli.BoolFlag{Name: "version, V"}

	app.Run(os.Args)
}

func command(c *cli.Context) error {
	// Pull arguments from the helper interpreter.
	var args cli.Args = c.Args()

	var start time.Time = time.Now()
	fmt.Println(fmt.Sprintf("Command: %s, starting: %s", c.Command.Name, time.Now()))

	options := mgocommands.NewBaseOptions(c.Bool("verbose"))
	command := mgocommands.CommandFactory(c.Command.Name, options)

	// Get argument count.
	argc := c.NArg()

	input := mgocommands.NewInputHandler()
	output := mgocommands.NewOutputHandler(os.Stdout, os.Stderr)

	// Check for pipe usage.
	pipe, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	} else if (pipe.Mode() & os.ModeNamedPipe) != 0 {
		if argc > 0 {
			return errors.New("File arguments and input pipes cannot be used simultaneously")
		}

		// Add stdin to the list of input files.
		input.AddHandle(os.Stdin)
	}

	// Loop through each argument and add files to the command.
	for i := 0; i < argc; i += 1 {
		path := args.Get(i)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			fmt.Println(fmt.Sprintf("%s skipped (%s)", path, err))
			continue
		}

		// Open the file and check for errors.
		file, err := os.OpenFile(path, os.O_RDONLY, 0)

		if err != nil {
			return err
		}

		input.AddHandle(file)
	}

	// Run the actual command.
	if err = mgocommands.RunCommand(command, input, output); err != nil {
		fmt.Println(fmt.Sprintf("Error: %s", err))
	}

	fmt.Println(fmt.Sprintf("Finished at %s (%s)", time.Now(), time.Since(start).String()))
	return err
}
