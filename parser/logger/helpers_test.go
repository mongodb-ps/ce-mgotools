package logger

import (
	"testing"

	"mgotools/parser/errors"
	"mgotools/util"
)

func TestPreamble(t *testing.T) {
	cmd, ns, op, err := Preamble(util.NewRuneReader("command test.$cmd command:"))
	if cmd != "command" || ns != "test.$cmd" || op != "command" || err != nil {
		t.Errorf("Values differ (%s, %s, %s, %s)", cmd, ns, op, err)
	}

	cmd, ns, op, err = Preamble(util.NewRuneReader("query test query:"))
	if err != errors.NoNamespaceFound {
		t.Errorf("Expected NoNamespaceFound, got %s", err)
	}

	cmd, ns, op, err = Preamble(util.NewRuneReader("command test.$cmd query"))
}
