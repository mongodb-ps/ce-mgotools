package commands

import "fmt"

type logFilter struct {
	baseOptions
}

func (f *logFilter) ParseLine(out chan<- string, in <-chan string, signal <-chan int) error {
	var exit int = 0
	go func() {
		exit = <-signal
	}()

	for logline := range in {
		switch {
		case exit != 0:
			// Received an exit signal so immediately exit.
			fmt.Println("Received exit signal")
			return nil

		default:
			out <- logline
			break

		}
	}

	return nil
}

func (f *logFilter) Finish(out chan<- string) error {
	return nil
}

func (f *logFilter) Match(in string) bool {

	return true
}
