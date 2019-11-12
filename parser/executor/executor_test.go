package executor

import (
	"testing"

	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"

	"github.com/pkg/errors"
)

func TestExecutor_appendKey(t *testing.T) {
	e := Executor{}
	e.appendKey(callback{Name: "bcd"})

	if len(e.executor) != 1 || e.executor[0].Name != "bcd" {
		t.Errorf("append to zero slice failed")
	}

	e.appendKey(callback{Name: "xyz"})
	if len(e.executor) != 2 || e.executor[1].Name != "xyz" {
		t.Errorf("append to end of slice failed")
	}

	e.appendKey(callback{Name: "def"})
	if len(e.executor) != 3 || e.executor[1].Name != "def" {
		t.Errorf("append to middle of slice failed")
	}

	e.appendKey(callback{Name: "abc"})
	if len(e.executor) != 4 || e.executor[0].Name != "abc" {
		t.Errorf("append to beginning of slice failed")
	}
}

func TestExecutor_Run(t *testing.T) {
	e := Executor{}
	unmatched := errors.New("unmatched")

	r := func(_ *internal.RuneReader) (message.Message, error) {
		return nil, nil
	}

	e.RegisterForReader("abc def", r)
	e.RegisterForReader("def ghi", r)
	e.RegisterForEntry("z", func(_ record.Entry, _ *internal.RuneReader) (message.Message, error) {
		return nil, nil
	})

	if _, err := e.Run(record.Entry{}, internal.NewRuneReader("abc def ghi jkl"), unmatched); err == unmatched {
		t.Error("failed to run 'abc def'")
	}

	if _, err := e.Run(record.Entry{}, internal.NewRuneReader("def ghi jkl"), unmatched); err == unmatched {
		t.Error("failed to run 'def ghi'")
	}

	if _, err := e.Run(record.Entry{}, internal.NewRuneReader("zyx abc"), unmatched); err == unmatched {
		t.Error("failed to run 'z'")
	}

	if _, err := e.Run(record.Entry{}, internal.NewRuneReader("ghi abc def"), unmatched); err != unmatched {
		t.Error("incorrect result for 'ghi abc def'")
	}

	if _, err := e.Run(record.Entry{}, internal.NewRuneReader("abc"), unmatched); err != unmatched {
		t.Error("incorrect result for 'abc'")
	}
}
