package message

import (
	"bytes"
	"strconv"
)

func (m Version) String() string {
	out := bytes.NewBuffer([]byte{})

	if m.Binary != "" {
		out.WriteString(m.Binary)
	}
	if m.Major > 0 {
		if m.Binary != "" {
			out.WriteRune(' ')
		}

		out.WriteString(strconv.Itoa(m.Major))
		out.WriteRune('.')
		out.WriteString(strconv.Itoa(m.Minor))

		if m.Revision > 0 {
			out.WriteRune('.')
			out.WriteString(strconv.Itoa(m.Revision))
		}
	}

	return out.String()
}

func BaseFromMessage(msg Message) (*BaseCommand, bool) {
	if msg == nil {
		return &BaseCommand{}, false
	}
	switch t := msg.(type) {
	case BaseCommand:
		return &t, true
	case Command:
		return &t.BaseCommand, true
	case CommandLegacy:
		return &t.BaseCommand, true
	case Operation:
		return &t.BaseCommand, true
	case OperationLegacy:
		return &t.BaseCommand, true
	case CRUD:
		return BaseFromMessage(t.Message)
	default:
		return &BaseCommand{}, false
	}
}

func PayloadFromMessage(msg Message) (*Payload, bool) {
	if msg == nil {
		return &Payload{}, false
	}
	switch t := msg.(type) {
	case Command:
		return &t.Payload, true
	case CommandLegacy:
		return &t.Payload, true
	case Operation:
		return &t.Payload, true
	case OperationLegacy:
		return &t.Payload, true
	default:
		return &Payload{}, false
	}
}

func MakeCommand() Command {
	return Command{
		BaseCommand: BaseCommand{
			Counters: make(map[string]int64),
		},
		Command: "",
		Payload: make(Payload),
		Locks:   make(map[string]interface{}),
	}
}

func MakeOperation() Operation {
	return Operation{
		BaseCommand: BaseCommand{
			Counters: make(map[string]int64),
		},
		Operation: "",
		Payload:   make(Payload),
		Locks:     make(map[string]interface{}),
	}
}

func MakeCommandLegacy() CommandLegacy {
	return CommandLegacy{
		BaseCommand: BaseCommand{
			Counters: make(map[string]int64),
		},
		Command: "",
		Payload: make(Payload),
		Locks:   make(map[string]int64),
	}
}

func MakeOperationLegacy() OperationLegacy {
	return OperationLegacy{
		BaseCommand: BaseCommand{
			Counters: make(map[string]int64),
		},
		Operation: "",
		Payload:   make(Payload),
		Locks:     make(map[string]int64),
	}
}
