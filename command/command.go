package command

// TODO: Stream input from _mongod_ and tee output
// TODO: Create better factory model than including parser.EntryFactory

import (
	"bufio"
	"errors"
	"io"
	"sync"

	"mgotools/record"
)

type Flag int

type commandSource <-chan record.Base
type commandTarget chan<- string
type commandError chan<- error
type commandHalt <-chan struct{}

type Input struct {
	Arguments ArgumentCollection
	Name      string
	Length    int64
	Reader    record.BaseFactory
}

type Output struct {
	Writer io.WriteCloser
	Error  io.WriteCloser
}

type Command interface {
	Finish(int) error
	Prepare(string, int, ArgumentCollection) error
	Run(int, commandTarget, commandSource, commandError, commandHalt)
	Terminate(chan<- string) error
}

// A method for preparing all the bytes and pieces to pass along to the next step.
func RunCommand(f Command, in []Input, out Output) error {
	var (
		// Keep a count of all contexts provided as input.
		count = len(in)

		// A multi-directional fan-out channel for catching and passing along errors.
		errorChannel = make(chan error)

		// An output channel that will facilitate moving data from commands to the output handle.
		outChannel = make(chan string)

		// A fatal channel to halt all input parsers.
		fatal = make(chan struct{})

		// A way to synchronize multiple goroutines.
		processSync sync.WaitGroup

		// A sync for multiple output handles (out, err)
		outputSync sync.WaitGroup

		// Create a helper to write to the output handle.
		outputWriter = bufio.NewWriter(out.Writer)

		// Create a helper to write to the error handle.
		errorWriter = bufio.NewWriter(out.Error)
	)

	// Always flush the output at the end of execution.
	defer outputWriter.Flush()
	defer errorWriter.Flush()

	if len(in) == 0 || out.Error == nil || out.Writer == nil {
		return errors.New("an input and output handler are required")
	}

	// Pass each file and its information to the command so it can prepare.
	for index, handle := range in {
		if err := f.Prepare(handle.Name, index, handle.Arguments); err != nil {
			return err
		}
	}

	// Synchronize the several goroutines created in this method.
	processSync.Add(count)

	// Signal any remaining processes to exit.
	defer close(fatal)
	go func() {
		<-fatal
	}()

	// There are two output syncs to wait for: output and errors.
	outputSync.Add(2)

	// Initiate a goroutine to wait for a single error and signal all other input parsing routines.
	go func() {
		// Wait for errors (or for the channel to close).
		defer outputSync.Done()

		for recv := range errorChannel {
			errorWriter.WriteString(recv.Error() + "\n")
			errorWriter.Flush()
		}
	}()

	go func() {
		// Create another goroutine for outputs. Start checking for output from the several input goroutines.
		// Output all received values directly (this may need to change in the future, i.e. should sorting be needed).
		defer outputSync.Done()

		for line := range outChannel {
			outputWriter.WriteString(line + "\n")
		}
	}()

	// Finally, a new goroutine is needed for each individual input file handle.
	for i := 0; i < count; i += 1 {
		go func(index int) {
			// Signal that this file is complete.
			defer processSync.Done()

			// Start a goroutine to wait each input file handle to finish processing.
			parseFile(f, index, in[index].Reader, outChannel, errorChannel, fatal)

			// Collect any final errors and send them along.
			if err := f.Finish(index); err != nil {
				errorChannel <- err
			}
		}(i)
	}

	// Wait for all input goroutines to finish.
	processSync.Wait()

	// Allow the command to finalize any pending actions.
	f.Terminate(outChannel)

	// Finalize the output processes by closing the out channel.
	close(outChannel)
	close(errorChannel)

	// Wait for all output goroutines to finish.
	outputSync.Wait()

	return nil
}

func parseFile(f Command, index int, in record.BaseFactory, out chan<- string, errs chan<- error, fatal chan struct{}) {
	var inputChannel = make(chan record.Base, 1024)
	var inputWaitGroup sync.WaitGroup

	// Count the number of goroutines that must complete before returning.
	inputWaitGroup.Add(2)

	go func() {
		// Decrement the wait group.
		defer inputWaitGroup.Done()

		// Close channels that will no longer be used after this method
		// exists (and signal any pending goroutines).
		defer close(inputChannel)

		for in.Next() {
			base, err := in.Get()
			if err == io.EOF {
				panic("eof error received before channel close")
			} else if err != nil {
				errs <- err
			} else {
				inputChannel <- base
			}
		}
	}()

	// Delegate line parsing to the individual commands.
	go func() {
		defer inputWaitGroup.Done()

		// Close the input file handle.
		defer in.Close()

		// Begin running the command.
		f.Run(index, out, inputChannel, errs, fatal)
	}()

	// Wait for both goroutines to complete.
	inputWaitGroup.Wait()
	return
}
