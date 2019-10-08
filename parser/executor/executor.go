package executor

import (
	"sort"

	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
)

type Type int

const (
	ReaderType = Type(iota)
	EntryType
)

type Reader func(*internal.RuneReader) (message.Message, error)
type Entry func(entry record.Entry, r *internal.RuneReader) (message.Message, error)

type callback struct {
	Name string
	Type Type

	reader Reader
	entry  Entry
}

type Executor struct {
	executor []callback
	peek     int
}

func New() *Executor {
	return &Executor{
		executor: make([]callback, 0),
	}
}

func (e *Executor) RegisterForReader(key string, f Reader) bool {
	if e.isKeyUsed(key) {
		return false
	}

	callback := callback{
		Name:   key,
		Type:   ReaderType,
		reader: f,
	}

	e.appendKey(callback)
	return true
}

func (e *Executor) RegisterForEntry(key string, f Entry) bool {
	if e.isKeyUsed(key) {
		return false
	}

	callback := callback{
		Name:  key,
		Type:  EntryType,
		entry: f,
	}

	e.appendKey(callback)
	return true
}

func (e *Executor) Run(entry record.Entry, r *internal.RuneReader, unmatched error) (message.Message, error) {
	preview := r.Peek(e.peek)

	pos, found := e.findPosition(preview)
	if !found {
		return nil, unmatched
	}

	callback := e.executor[pos]

	switch callback.Type {
	case ReaderType:
		return callback.reader(r)
	case EntryType:
		return callback.entry(entry, r)
	default:
		panic("unknown type encountered in executor")
	}
}

func (e *Executor) appendKey(f callback) {
	key := f.Name

	if len(key) == 0 {
		panic("attempted to register with an empty key")
	}

	if len(key) > e.peek {
		e.peek = len(key)
	}

	index, ok := e.findPosition(key)
	if ok {
		panic("attempt to register a duplicate key")
	}

	e.executor = append(e.executor, callback{})
	copy(e.executor[index+1:], e.executor[index:])
	e.executor[index] = f
}

func (e *Executor) findPosition(key string) (pos int, ok bool) {
	if e.executor == nil {
		e.executor = make([]callback, 0)
	}

	pos = sort.Search(len(e.executor), func(index int) bool {
		if len(e.executor[index].Name) <= len(key) && e.executor[index].Name >= key[:len(e.executor[index].Name)] {
			return true
		}

		return e.executor[index].Name >= key
	})

	ok = pos < len(e.executor) && len(key) >= len(e.executor[pos].Name) && key[:len(e.executor[pos].Name)] == e.executor[pos].Name
	return
}

func (e *Executor) isKeyUsed(key string) bool {
	_, ok := e.findPosition(key)
	return ok
}
