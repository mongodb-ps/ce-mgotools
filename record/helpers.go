package record

import (
	"bytes"
	"strconv"
)

func (m MsgVersion) String() string {
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

func MsgBaseFromMessage(msg Message) (*MsgBase, bool) {
	if msg == nil {
		return &MsgBase{}, false
	}
	switch t := msg.(type) {
	case MsgBase:
		return &t, true
	case MsgCommand:
		return &t.MsgBase, true
	case MsgCommandLegacy:
		return &t.MsgBase, true
	case MsgOperation:
		return &t.MsgBase, true
	case MsgOperationLegacy:
		return &t.MsgBase, true
	default:
		return &MsgBase{}, false
	}
}

func MsgPayloadFromMessage(msg Message) (*MsgPayload, bool) {
	if msg == nil {
		return &MsgPayload{}, false
	}
	switch t := msg.(type) {
	case MsgCommand:
		return &t.Payload, true
	case MsgCommandLegacy:
		return &t.Payload, true
	case MsgOperation:
		return &t.Payload, true
	case MsgOperationLegacy:
		return &t.Payload, true
	default:
		return &MsgPayload{}, false
	}
}

func MakeMsgCommand() MsgCommand {
	return MsgCommand{
		MsgBase: MsgBase{
			Counters: make(map[string]int64),
		},
		Command: "",
		Payload: make(MsgPayload),
		Locks:   make(map[string]interface{}),
	}
}

func MakeMsgOperation() MsgOperation {
	return MsgOperation{
		MsgBase: MsgBase{
			Counters: make(map[string]int64),
		},
		Operation: "",
		Payload:   make(MsgPayload),
		Locks:     make(map[string]interface{}),
	}
}

func MakeMsgCommandLegacy() MsgCommandLegacy {
	return MsgCommandLegacy{
		MsgBase: MsgBase{
			Counters: make(map[string]int64),
		},
		Command: "",
		Payload: make(MsgPayload),
		Locks:   make(map[string]int64),
	}
}

func MakeMsgOperationLegacy() MsgOperationLegacy {
	return MsgOperationLegacy{
		MsgBase: MsgBase{
			Counters: make(map[string]int64),
		},
		Operation: "",
		Payload:   make(MsgPayload),
		Locks:     make(map[string]int64),
	}
}
