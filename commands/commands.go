package commands

import (
	"bufio"
	"errors"
	"fmt"
	"compress/gzip"
	"log"
	"os"
	synclib "sync"
	"mgotools/parser"
)

type Command interface {
	ParseLine(chan<- string, <-chan string, <-chan int, *parser.LogContext) error
	Finish(chan<- string) error
}

type baseCommandFileHandle struct {
	Closed      bool
	CloseSignal chan int
	FileHandle  *os.File
	LogContext  *parser.LogContext
}

type BaseOptions struct {
	LinearParse     bool
	TimestampFormat string
	Verbose         bool
}

type inputHandler struct {
	in   []baseCommandFileHandle
	sync synclib.WaitGroup
}

type outputHandler struct {
	out baseCommandFileHandle
	err baseCommandFileHandle
	log *log.Logger
}

func NewInputHandler() *inputHandler {
	return &inputHandler{sync: synclib.WaitGroup{}}
}

func NewOutputHandler(out *os.File, err *os.File) *outputHandler {
	return &outputHandler{
		out: baseCommandFileHandle{
			CloseSignal: make(chan int),
			Closed:      false,
			FileHandle:  out,
		},

		err: baseCommandFileHandle{
			CloseSignal: make(chan int),
			Closed:      false,
			FileHandle:  out,
		},

		log: log.New(err, "", 0),
	}
}

func (i *inputHandler) AddHandle(reader *os.File) {
	commandFileHandle := baseCommandFileHandle{
		CloseSignal: make(chan int),
		Closed:      false,
		FileHandle:  reader,
		LogContext:  parser.NewLogContext(),
	}

	go commandFileHandle.closeHandler(&i.sync)

	i.sync.Add(1)
	i.in = append(i.in, commandFileHandle)
}

// General purpose close handler for input and output handles.
func (b *baseCommandFileHandle) closeHandler(sync *synclib.WaitGroup) {
	<-b.CloseSignal

	if !b.Closed {
		fmt.Println("Close signal received, closing file")
		b.Closed = true
		b.FileHandle.Close()

		if sync != nil {
			sync.Done()
		}
	}
}

// A general purpose method to create different types of command structures.
func CommandFactory(command string, options BaseOptions) Command {
	switch command {
	case "filter":
		return &logFilter{
			BaseOptions: options,
		}

	default:
		panic("unexpected command received")
	}
}

// A method for preparing all the bytes and pieces to pass along to the next step.
func RunCommand(f Command, in *inputHandler, out *outputHandler) error {
	// A count of inputs.
	var count int = len(in.in)

	// A generic error that does *not* get used by input/output goroutines.
	var err error

	// A multi-directional fan-out channel for catching and passing along errors.
	var errorFanoutChannel chan error = make(chan error)

	// An output channel that will facilitate moving data from commands to the output handle.
	var outChannel chan string = make(chan string)

	// A signal channel to halt all input parsers.
	var signal []chan int = make([]chan int, count)

	// A way to synchronize multiple goroutines.
	var sync synclib.WaitGroup

	// Create a helper to write to the output handle.
	var writer *bufio.Writer = bufio.NewWriter(out.out.FileHandle)
	defer writer.Flush()

	if in == nil || out == nil {
		return errors.New("An input and output handler are required")
	}

	// Synchronize the several goroutines created in this method.
	sync.Add(count)

	// Close synchronization channels once the method exits.
	defer close(errorFanoutChannel)

	// Initiate a goroutine to wait for a single error and signal all other input parsing routines.
	go func() {
		// Wait for errors (or for the channel to close).
		err = <-errorFanoutChannel
		if err == nil {
			// No errors mean the channel closed without issue.
			fmt.Println(fmt.Sprintf("No errors received, ending signal goroutine"))
			return
		}

		for i := 0; i < count; i += 1 {
			// Signal to other running tasks to halt.
			fmt.Println(fmt.Sprintf("Error! %s", err))
			signal[i] <- 1
		}
	}()

	// Finally, a new goroutine is needed for each individual input file handle.
	for i := 0; i < count; i += 1 {
		// Create a signal for input handler _i_.
		signal[i] = make(chan int)

		// Start a goroutine to wait each input file handle to finish processing.
		go parseFile(f, &in.in[i], outChannel, errorFanoutChannel, signal[i], &sync)
	}

	go func() {
		// Wait for all input goroutines to finish.
		sync.Wait()

		// Allow the command to finalize any pending actions.
		if err == nil {
			f.Finish(outChannel)
		}

		// Finalize the output process by closing the out channel.
		close(outChannel)
	}()

	// Create another goroutine for outputs. Start checking for output from the several input goroutines.
	// Output all received values directly (this may need to change in the future, i.e. should sorting be needed).
	for line := range outChannel {
		writer.WriteString(line + "\n")
	}

	// Wait for all file handles to finish closing.
	in.sync.Wait()

	return err
}

func parseFile(f Command, in *baseCommandFileHandle, out chan<- string, err chan<- error, signal chan int, sync *synclib.WaitGroup) {
	var (
		inputChannel chan string = make(chan string, 1024)
	)

	// Close channels that will no longer be used after this method exists (and signal any pending goroutines).
	defer close(inputChannel)
	defer close(signal)

	// Close the input file handle.
	defer func() { in.CloseSignal <- 1 }()

	// Delegate line parsing to the individual commands.
	go func() {
		f.ParseLine(out, inputChannel, signal, in.LogContext)

		// Alert the synchronization object that one of the goroutines is finished.
		defer sync.Done()
	}()

	reader := bufio.NewReader(in.FileHandle)
	scanner := bufio.NewScanner(reader)

	if peek, err := reader.Peek(2); err == nil {
		if peek[0] == 0x1f && peek[1] == 0x8b {
			if gzipReader, err := gzip.NewReader(reader); err == nil {
				scanner = bufio.NewScanner(gzipReader)
			} else {
				fmt.Println(err)
			}
		}
	}

	for scanner.Scan() {
		if text := scanner.Text(); text != "" {
			inputChannel <- text
		}
	}

	if scannerError := scanner.Err(); scanner != nil {
		err <- scannerError
	}
}
