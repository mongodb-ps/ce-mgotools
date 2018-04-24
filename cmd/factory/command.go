package factory

// TODO: Stream input from _mongod_ and tee output
// TODO: Create better factory model than including parser.EntryFactory

import (
	"bufio"
	"compress/gzip"
	"errors"
	"os"
	synclib "sync"

	"mgotools/util"
)

type Command interface {
	Finish(int) error
	Prepare(InputContext) error
	ProcessLine(int, chan<- string, <-chan string, chan<- error, <-chan struct{}) error
	Terminate(chan<- string) error
}

type CommandFlag int

type baseCommandFileHandle struct {
	Closed      bool
	CloseSignal chan struct{}
	FileHandle  *os.File
	Name        string
}
type BaseOptions struct {
	DateFormat  string
	LinearParse bool
	Verbose     bool
}
type inputHandler struct {
	in []struct {
		base baseCommandFileHandle
		args CommandArgumentCollection
	}
	sync synclib.WaitGroup
}
type InputContext struct {
	Index int
	Name  string
	CommandArgumentCollection
}
type outputHandler struct {
	out baseCommandFileHandle
	err baseCommandFileHandle
}

func NewInputHandler() *inputHandler {
	return &inputHandler{sync: synclib.WaitGroup{}}
}

func NewOutputHandler(out *os.File, err *os.File) *outputHandler {
	return &outputHandler{
		out: baseCommandFileHandle{
			CloseSignal: make(chan struct{}),
			Closed:      false,
			FileHandle:  out,
		},

		err: baseCommandFileHandle{
			CloseSignal: make(chan struct{}),
			Closed:      false,
			FileHandle:  err,
		},
	}
}

func (i *inputHandler) AddHandle(name string, reader *os.File, args CommandArgumentCollection) {
	commandFileHandle := baseCommandFileHandle{
		CloseSignal: make(chan struct{}),
		Closed:      false,
		FileHandle:  reader,
		Name:        name,
	}

	go commandFileHandle.closeHandler(&i.sync)

	i.sync.Add(1)
	i.in = append(i.in, struct {
		base baseCommandFileHandle
		args CommandArgumentCollection
	}{commandFileHandle, args})
}

// General purpose close handler for input and output handles.
func (b *baseCommandFileHandle) closeHandler(sync *synclib.WaitGroup) {
	<-b.CloseSignal

	if !b.Closed {
		b.Closed = true
		b.FileHandle.Close()

		if sync != nil {
			sync.Done()
		}
	}
}

// A method for preparing all the bytes and pieces to pass along to the next step.
func RunCommand(f Command, in *inputHandler, out *outputHandler) error {
	var (
		// A count of inputs.
		count int = len(in.in)

		// A generic error that does *not* get used by input/output goroutines.
		err error

		// A multi-directional fan-out channel for catching and passing along errors.
		errorChannel chan error = make(chan error)

		// An output channel that will facilitate moving data from commands to the output handle.
		outChannel chan string = make(chan string)

		// A fatal channel to halt all input parsers.
		fatal chan struct{} = make(chan struct{})

		// A way to synchronize multiple goroutines.
		processSync synclib.WaitGroup

		// A sync for multiple output handles (out, err)
		outputSync synclib.WaitGroup

		// Create a helper to write to the output handle.
		outputWriter = bufio.NewWriter(out.out.FileHandle)

		// Create a helper to write to the error handle.
		errorWriter = bufio.NewWriter(out.err.FileHandle)
	)

	// Always flush the output at the end of execution.
	defer outputWriter.Flush()
	defer errorWriter.Flush()

	if in == nil || out == nil {
		return errors.New("an input and output handler are required")
	}

	// Pass each file and its information to the command so it can prepare.
	for index, handle := range in.in {
		if err = f.Prepare(InputContext{index, handle.base.Name, handle.args}); err != nil {
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

	// Initiate a goroutine to wait for a single error and signal all other input parsing routines.
	go func() {
		// Wait for errors (or for the channel to close).
		outputSync.Add(1)
		defer outputSync.Done()
		for recv := range errorChannel {
			errorWriter.WriteString(recv.Error() + "\n")
			errorWriter.Flush()
		}
	}()

	go func() {
		// Create another goroutine for outputs. Start checking for output from the several input goroutines.
		// Output all received values directly (this may need to change in the future, i.e. should sorting be needed).
		outputSync.Add(1)
		defer outputSync.Done()
		for line := range outChannel {
			outputWriter.WriteString(line + "\n")
		}
	}()

	// Finally, a new goroutine is needed for each individual input file handle.
	for i := 0; i < count; i += 1 {
		// Start a goroutine to wait each input file handle to finish processing.
		go parseFile(f, i, &in.in[i].base, outChannel, errorChannel, fatal, &processSync)
	}

	// Wait for all file handles to finish closing.
	in.sync.Wait()

	// Wait for all input goroutines to finish.
	processSync.Wait()

	// Finalize the output processes by closing the out channel.
	close(outChannel)
	close(errorChannel)

	// Wait for all output goroutines to finish.
	outputSync.Wait()

	// Allow the command to finalize any pending actions.
	if err == nil {
		f.Terminate(outChannel)
	}

	return err
}

func parseFile(f Command, index int, in *baseCommandFileHandle, out chan<- string, errs chan<- error, fatal chan struct{}, sync *synclib.WaitGroup) {
	var inputChannel chan string = make(chan string, 1024)

	// Close channels that will no longer be used after this method exists (and signal any pending goroutines).
	defer close(inputChannel)

	// Alert the synchronization object that one of the goroutines is finished.
	defer sync.Done()

	// Delegate line parsing to the individual commands.
	go func() {
		sync.Add(1)
		defer sync.Done()
		// Close the input file handle.
		defer func() { in.CloseSignal <- struct{}{} }()
		// Begin running the command.
		f.ProcessLine(index, out, inputChannel, errs, fatal)
	}()

	reader := bufio.NewReader(in.FileHandle)
	scanner, err := checkGZip(reader, bufio.NewScanner(reader))
	if err != nil {
		errs <- err
		close(fatal)
		return
	}

	for scanner.Scan() {
		if text := scanner.Text(); text != "" {
			inputChannel <- text
		}
	}

	if scannerError := scanner.Err(); scannerError != nil {
		util.Debug("error: %s", scannerError)
		errs <- scannerError
	}

	if err := f.Finish(index); err != nil {
		errs <- err
	}
	util.Debug("end of parseFile")
	return
}

func checkGZip(reader *bufio.Reader, scanner *bufio.Scanner) (*bufio.Scanner, error) {
	if peek, err := reader.Peek(2); err == nil {
		if peek[0] == 0x1f && peek[1] == 0x8b {
			if gzipReader, err := gzip.NewReader(reader); err == nil {
				scanner = bufio.NewScanner(gzipReader)
			} else {
				return nil, err
			}
		}
	}
	return scanner, nil
}
