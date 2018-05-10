package record

import (
	"bytes"
)

const MaxBufferSize = 16777216

type AccumulatorResult struct {
	Base  Base
	Error error
}

// The accumulator is designed to concat multi-line entries. The log format does
// not properly escape quotes or newline entries, which causes problems when
// lines are analyzed one at a time.
//
// Solving this problem requires a start marker, which is the date in all
// versions of MongoDB. However, the date in older versions is a CString.
// Thankfully, the record.Base object contains enough information to properly
// parse multi-line input.
func Accumulator(in <-chan string, out chan<- AccumulatorResult, callback func(string, uint) (Base, error)) {
	defer close(out) // Last defer called.

	type accumulator struct {
		count   int
		last    []AccumulatorResult
		size    int
		started bool
	}

	reset := func(a *accumulator) {
		a.last = a.last[:0]
		a.size = 0
	}

	flush := func(a *accumulator) {
		for _, r := range a.last {
			out <- r
		}
		reset(a)
	}

	a := accumulator{
		count:   0,
		last:    make([]AccumulatorResult, 0),
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
					// Create a buffer to construct a single line containing
					// every accumulated record.Base entry between the latest
					// line and the next line. Disregard any errors.
					accumulator := bytes.NewBuffer([]byte{})
					for _, r := range a.last {
						accumulator.WriteString(r.Base.String())
						accumulator.WriteRune('\n')
					}

					// Create a record.Base object with all the accumulated base
					// objects from previous lines.
					s := accumulator.String()
					s = s[:len(s)-1] // Remove the extraneous newline.

					m, err := callback(s, a.last[0].Base.LineNumber)
					reset(&a)

					out <- AccumulatorResult{
						Base:  m,
						Error: err,
					}
				}
			}

			// Started is not set until the first time a valid date is encountered.
			a.started = true

			// Keep the last entry and output nothing (for now).
			a.size += base.Length()
			a.last = append(a.last, AccumulatorResult{
				Base:  base,
				Error: err,
			})
		} else {
			if !a.started {
				// No date line has been discovered so the log is either invalid
				// or started in the middle of a multi-line string. Either case
				// demands simply outputting the erratic result.
				out <- AccumulatorResult{
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
				a.last = append(a.last, AccumulatorResult{base, err})
				a.size += base.Length()
			}
		}
	}
}
