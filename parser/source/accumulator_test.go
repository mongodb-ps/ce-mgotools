package source

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

type accumulatorFile struct {
	Reader   string
	Expected []string
}

func TestAccumulator_Run(tr *testing.T) {
	//tr.Parallel()

	tests := map[string]accumulatorFile{
		"FileInvalid":       fileInvalid,
		"FileIso1Valid":     fileIso1Valid,
		"FileIso1Multiline": fileIso1MultiLine,
	}

	for name, reader := range tests {
		tr.Run(name, func(t *testing.T) {
			testAccumulator(reader, t)
		})
	}
}

func testAccumulator(r accumulatorFile, t *testing.T) {
	scanner := bufio.NewScanner(bytes.NewBufferString(r.Reader))

	in, out := make(chan string), make(chan accumulatorResult)
	go Accumulator(in, out, Log{}.NewBase)

	go func() {
		i := uint(0)
		for scanner.Scan() {
			i += 1
			in <- scanner.Text()
		}
		close(in)
	}()

	i := int(0)
	for m := range out {
		if m.Error == io.EOF {
			break
		}
		if i >= len(r.Expected) {
			t.Errorf("Too many results found")
			return
		}
		if m.Base.String() != r.Expected[i] {
			t.Errorf("Line %d does not match", i+1)
			t.Errorf("Got (%d): %s", m.Base.Length(), m.Base.String())
			t.Errorf("Is  (%d): %s", len(r.Expected[i]), r.Expected[i])
		}
		i += 1
	}

	if i < len(r.Expected) {
		t.Errorf("Too few lines returned, %d of %d", i, len(r.Expected))
	}
}

var fileIso1Valid = accumulatorFile{
	Reader: `2018-01-16T15:00:44.732-0800 I STORAGE  [signalProcessingThread] closeAllFiles() finished
2018-01-16T15:00:44.733-0800 I STORAGE  [signalProcessingThread] shutdown: removing fs lock...
2018-01-16T15:00:44.733-0800 I CONTROL  [signalProcessingThread] now exiting
2018-01-16T15:00:44.734-0800 I CONTROL  [signalProcessingThread] shutting down with code:0`,

	Expected: []string{
		"2018-01-16T15:00:44.732-0800 I STORAGE  [signalProcessingThread] closeAllFiles() finished",
		"2018-01-16T15:00:44.733-0800 I STORAGE  [signalProcessingThread] shutdown: removing fs lock...",
		"2018-01-16T15:00:44.733-0800 I CONTROL  [signalProcessingThread] now exiting",
		"2018-01-16T15:00:44.734-0800 I CONTROL  [signalProcessingThread] shutting down with code:0",
	},
}

var fileIso1MultiLine = accumulatorFile{
	Reader: `2018-01-16T15:00:44.569-0800 I COMMAND  [conn1] command test.$cmd command: isMaster { isMaster: 1.0, forShell: 1.0 } numYields:0 reslen:174 locks:{} protocol:op_command 0ms
2018-01-16T15:00:44.571-0800 I COMMAND  [conn1] command test.foo command: find { find: "foo", filter: { c: "this is a string "with quotes" and
newlines

done." } } planSummary: IXSCAN { c: 1 } keysExamined:0 docsExamined:0 cursorExhausted:1 numYields:0 nreturned:0 reslen:81 locks:{ Global: { acquireCount: { r: 2 } }, MMAPV1Journal: { acquireCount: { r: 1 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { R: 1 } } } protocol:op_command 0ms
2018-01-16T15:00:44.572-0800 I COMMAND  [conn1] command test.$cmd command: isMaster { isMaster: 1.0, forShell: 1.0 } numYields:0 reslen:174 locks:{} protocol:op_command 0ms
2018-01-16T15:00:44.573-0800 I COMMAND  [conn1] command test.foo command: explain { explain: { find: "foo", filter: { b: 1.0 } }, verbosity: "queryPlanner" } numYields:0 reslen:381 locks:{ Global: { acquireCount: { r: 2 } }, MMAPV1Journal: { acquireCount: { r: 1 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { R: 1 } } } protocol:op_command 0ms`,

	Expected: []string{
		`2018-01-16T15:00:44.569-0800 I COMMAND  [conn1] command test.$cmd command: isMaster { isMaster: 1.0, forShell: 1.0 } numYields:0 reslen:174 locks:{} protocol:op_command 0ms`,
		`2018-01-16T15:00:44.571-0800 I COMMAND  [conn1] command test.foo command: find { find: "foo", filter: { c: "this is a string "with quotes" and
newlines

done." } } planSummary: IXSCAN { c: 1 } keysExamined:0 docsExamined:0 cursorExhausted:1 numYields:0 nreturned:0 reslen:81 locks:{ Global: { acquireCount: { r: 2 } }, MMAPV1Journal: { acquireCount: { r: 1 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { R: 1 } } } protocol:op_command 0ms`,
		`2018-01-16T15:00:44.572-0800 I COMMAND  [conn1] command test.$cmd command: isMaster { isMaster: 1.0, forShell: 1.0 } numYields:0 reslen:174 locks:{} protocol:op_command 0ms`,
		`2018-01-16T15:00:44.573-0800 I COMMAND  [conn1] command test.foo command: explain { explain: { find: "foo", filter: { b: 1.0 } }, verbosity: "queryPlanner" } numYields:0 reslen:381 locks:{ Global: { acquireCount: { r: 2 } }, MMAPV1Journal: { acquireCount: { r: 1 } }, Database: { acquireCount: { r: 1 } }, Collection: { acquireCount: { R: 1 } } } protocol:op_command 0ms`,
	},
}

var fileInvalid = accumulatorFile{
	Reader: `line 1
line 2
line 3
line 4`,

	Expected: []string{
		"line 1",
		"line 2",
		"line 3",
		"line 4",
	},
}
