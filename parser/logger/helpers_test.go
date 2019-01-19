package logger

import (
	"fmt"
	"reflect"
	"testing"

	"mgotools/internal"
	"mgotools/record"
	"mgotools/util"
)

func TestCheckCounterVersionError(t *testing.T) {
	type Tuple struct {
		E error
		V error
	}

	type Result struct {
		C bool
		E error
	}

	v := internal.VersionUnmatched{""}
	s := map[Tuple]Result{
		{internal.CounterUnrecognized, v}: {true, v},

		{internal.UnexpectedEOL, v}: {false, internal.UnexpectedEOL},

		{nil, v}: {false, nil},
	}

	for m, r := range s {
		c, e := CheckCounterVersionError(m.E, m.V)
		if c != r.C || e != r.E {
			t.Errorf("Expected (%v, %s), got (%v, %s)", r.C, r.E, c, e)
		}
	}
}

func TestCommandPreamble(t *testing.T) {
	type PreambleResult struct {
		Cmd     string
		Ns      string
		Agent   string
		Payload record.MsgPayload
		Err     error
	}

	s := map[string]PreambleResult{
		`command test.$cmd appName: "MongoDB Shell" command: isMaster { isMaster: 1, forShell: 1 }`: {"isMaster", "test.$cmd", "MongoDB Shell", record.MsgPayload{"isMaster": 1, "forShell": 1}, nil},

		`command test.$cmd command: isMaster { isMaster: 1, forShell: 1 }`: {"isMaster", "test.$cmd", "", record.MsgPayload{"isMaster": 1, "forShell": 1}, nil},

		`command test.$cmd planSummary: IXSCAN { a: 1 }`: {"", "test.$cmd", "", record.MsgPayload{}, nil},

		`command test.$cmd command: drop { drop: "$cmd" }`: {"drop", "test.$cmd", "", record.MsgPayload{"drop": "$cmd"}, nil},

		`command test command: dropDatabase { dropDatabase: 1 }`: {"dropDatabase", "test", "", record.MsgPayload{"dropDatabase": 1}, nil},

		`command test.$cmd command: { a: 1 }`: {"command", "test.$cmd", "", record.MsgPayload{"a": 1}, nil},

		`command test.$cmd`: {"", "", "", nil, internal.UnexpectedEOL},

		`command test.$cmd appName: "...`: {"", "", "", nil, fmt.Errorf("unexpected end of string looking for quote (\")")},

		`query test.$cmd query: { a: 1 }`: {"", "", "", nil, internal.CommandStructure},

		`command test.$cmd command:`: {"", "", "", nil, internal.CommandStructure},
	}

	for m, r := range s {
		cmd, err := CommandPreamble(util.NewRuneReader(m))
		if (err != nil && r.Err == nil) || (err == nil && r.Err != nil) || err != nil && r.Err != nil && err.Error() != r.Err.Error() {
			t.Errorf("Error mismatch: expected '%s', got '%s'", r.Err, err)
		}

		if cmd.Command != r.Cmd {
			t.Errorf("Command mismatch: expected '%s', got '%s'", r.Cmd, cmd.Command)
		}

		if cmd.Namespace != r.Ns {
			t.Errorf("Namespace mismatch: expected '%s', got '%s'", r.Ns, cmd.Namespace)
		}

		if cmd.Agent != r.Agent {
			t.Errorf("Agent mismatch: expected '%s', got '%s'", r.Agent, cmd.Agent)
		}

		if !reflect.DeepEqual(cmd.Payload, r.Payload) {
			t.Errorf("Payloads differ: \t%#v\n\t%#v", r.Payload, cmd.Payload)
		}
	}
}

func TestDuration(t *testing.T) {
	type R struct {
		N int64
		E error
	}
	s := map[string]R{
		`10ms`: {10, nil},
		`0ms`:  {0, nil},
		`-1ms`: {0, nil},
		``:     {0, internal.UnexpectedEOL},
		`ok`:   {0, internal.MisplacedWordException},
	}
	for m, r := range s {
		n, e := Duration(util.NewRuneReader(m))
		if n != r.N || e != r.E {
			t.Errorf("Expected (%v, %s), got (%v, %s)", r.N, r.E, n, e)
		}
	}
}

func TestPreamble(t *testing.T) {
	cmd, ns, op, err := Preamble(util.NewRuneReader("command test.$cmd command:"))
	if cmd != "command" || ns != "test.$cmd" || op != "command" || err != nil {
		t.Errorf("Values differ (%s, %s, %s, %s)", cmd, ns, op, err)
	}

	cmd, ns, op, err = Preamble(util.NewRuneReader("query test query:"))
	if cmd != "query" || ns != "test" || op != "query" || err != nil {
		t.Errorf("Expected 'test', got %s (err: %s)", ns, err)
	}

	cmd, ns, op, err = Preamble(util.NewRuneReader("update test.$cmd query:"))
	if cmd != "update" || ns != "test.$cmd" || op != "query" || err != nil {
		t.Errorf("Values differ (%s, %s, %s, %s)", cmd, ns, op, err)
	}

	cmd, ns, op, err = Preamble(util.NewRuneReader("command test.$cmd appName: \"agent\" command:"))
	if cmd != "command" || ns != "test.$cmd" || op != "appName" || err != nil {
		t.Errorf("Preamble failed, got (cmd: %s, ns: %s, op: %s, err: %s)", cmd, ns, op, err)
	}

	cmd, ns, op, err = Preamble(util.NewRuneReader("command test.$cmd query"))
	if cmd != "command" || ns != "test.$cmd" || op != "query" || err != nil {
		t.Errorf("Preamble failed, got (cmd: %s, ns: %s, op: %s, err: %s)", cmd, ns, op, err)
	}

	cmd, ns, op, err = Preamble(util.NewRuneReader(""))
	if err != internal.UnexpectedEOL {
		t.Errorf("Expected UnexpectedEOL error, got %s", err)
	}
}

func TestOperationPreamble(t *testing.T) {
	op, err := OperationPreamble(util.NewRuneReader("insert test.$cmd query: { a: 1 }"))
	if op.Operation != "insert" || op.Namespace != "test.$cmd" || err != nil {
		t.Errorf("Values differ (%s, %s, %s)", op.Operation, op.Namespace, err)
	}
}
