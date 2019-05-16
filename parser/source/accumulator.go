package source

import (
	"bufio"
	"bytes"
	"io"

	"mgotools/parser/record"
)

const OutputBuffer = 128
const MaxBufferSize = 16777216

type accumulatorReadCloser interface {
	io.ReadCloser
	NewBase(string, uint) (record.Base, error)
}

type accumulator struct {
	io.Closer

	eof   bool
	next  record.Base
	error error

	Log *bufio.Scanner
	Out chan accumulatorResult
	In  chan string
}

var _ io.ReadCloser = (*accumulator)(nil)
var _ Factory = (*accumulator)(nil)

type accumulatorResult struct {
	Base  record.Base
	Error error
}

func NewAccumulator(handle accumulatorReadCloser) *accumulator {
	r := &accumulator{
		Closer: handle,
		eof:    false,

		Log: bufio.NewScanner(handle),
		Out: make(chan accumulatorResult, OutputBuffer),
		In:  make(chan string),
	}

	// Begin scanning the source and send it to the input channel.
	go func() {
		defer close(r.In)

		for r.Log.Scan() {
			r.In <- r.Log.Text()
		}
	}()

	go Accumulator(r.In, r.Out, handle.NewBase)
	return r
}

// The accumulator is designed to concat multi-line entries. The log format does
// not properly escape quotes or newline entries, which causes problems when
// lines are analyzed one at a time.
//
// Solving this problem requires a start marker, which is the date in all
// versions of MongoDB. However, the date in older versions is a CString.
// Thankfully, the record.Base object contains enough information to properly
// parse multi-line input.
func Accumulator(in <-chan string, out chan<- accumulatorResult, callback func(string, uint) (record.Base, error)) {
	defer func() {
		// Last defer called.
		close(out)
	}()

	type accumulatorCounter struct {
		count   int
		last    []accumulatorResult
		size    int
		started bool
	}

	accumulate := func(a accumulatorCounter) string {
		// Create a buffer to construct a single line containing
		// every accumulated record.Base entry between the latest
		// line and the next line. Disregard any errors.
		accumulator := bytes.NewBuffer(make([]byte, 0, a.size+len(a.last)))
		for _, r := range a.last {
			accumulator.WriteString(r.Base.String())
			accumulator.WriteRune('\n')
		}
		// Create a record.Base object with all the accumulated base
		// objects from previous lines.
		s := accumulator.String()
		s = s[:len(s)-1]
		// Remove the extraneous newline.
		return s
	}

	reset := func(a *accumulatorCounter) {
		a.last = a.last[:0]
		a.size = 0
	}

	flush := func(a *accumulatorCounter) {
		for _, r := range a.last {
			out <- r
		}
		reset(a)
	}

	a := accumulatorCounter{
		count:   0,
		last:    make([]accumulatorResult, 0),
		size:    0,
		started: false,
	}

	defer flush(&a)
	lineNumber := uint(0)

	for line := range in {
		lineNumber += 1
		base, err := callback(line, lineNumber)

		if base.RawDate != "" {
			// The current object has a valid date and thus starts a new log
			// line that _might_ span multiple lines. That means the previous
			// line containing a date does not span multiple lines. Check
			// whether a.last contains a value and output the value.
			if a.size > 0 {
				if len(a.last) == 1 {
					out <- a.last[0]
					reset(&a)
				} else {
					// Handle the actual accumulation and generate a string. The
					// string gets passed back to the callback method to create
					// a new object.
					s := accumulate(a)

					// Create a base object from the newly accumulated string.
					m, err := callback(s, a.last[0].Base.LineNumber)
					reset(&a)

					// Send the completed output and any errors.
					out <- accumulatorResult{
						Base:  m,
						Error: err,
					}
				}
			}

			// Started is not set until the first time a valid date is encountered.
			a.started = true

			// Keep the last entry and output nothing (for now).
			a.size += base.Length()
			a.last = append(a.last, accumulatorResult{
				Base:  base,
				Error: err,
			})
		} else {
			if !a.started {
				// No date line has been discovered so the log is either invalid
				// or started in the middle of a multi-line string. Either case
				// demands simply outputting the erratic result.
				out <- accumulatorResult{
					Base:  base,
					Error: err,
				}
			} else if a.size > MaxBufferSize {
				// The buffer is too large so create a base object and try to do
				// something with it. The maximum object size is 16MB but logs
				// get truncated well before that, so this should be something
				// reasonable but less than or equal to 16MB
				flush(&a)
			} else {
				// Add each line to the accumulator array and keep track of how
				// many line bytes are being stored.
				a.last = append(a.last, accumulatorResult{base, err})
				a.size += base.Length()
			}
		}
	}

	// It's possible that the log ended (for example, a stack trace that spans
	// multiple lines then terminates). A check is needed for pending lines
	// only when the first line is valid; otherwise the accumulated errors get
	// flushed directly.
	if len(a.last) > 1 && a.last[0].Error == nil {
		// Grab the accumulated string.
		s := accumulate(a)

		// Generate a base object based on the string.
		m, err := callback(s, a.last[0].Base.LineNumber)
		reset(&a)

		out <- accumulatorResult{
			Base:  m,
			Error: err,
		}
	}
}

// Implement a reader that returns a byte array of the most current accumulated
// line.
func (f *accumulator) Read(p []byte) (n int, err error) {
	if f.eof {
		return 0, f.error
	}
	s := []byte(f.next.String())
	l := len(s)

	if l > cap(p) {
		panic("buffer too small for next set of results")
	}

	copy(p, s)
	return l, nil
}

func (f *accumulator) Next() bool {
	if f.eof {
		return false
	}

	b, ok := <-f.Out
	f.next = b.Base
	f.error = b.Error

	if !ok {
		f.eof = true
		return false
	}
	return true
}

func (f *accumulator) Get() (record.Base, error) {
	return f.next, f.error
}

func (f *accumulator) Close() error {
	return f.Closer.Close()
}
