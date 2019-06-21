package parser

import (
	"net"
	"reflect"
	"testing"

	"mgotools/internal"
	"mgotools/parser/message"
)

func TestMongod_Network(t *testing.T) {
	valid := map[string]message.Message{
		"connection accepted from 127.0.0.1:27017 #1": message.Connection{Address: net.IPv4(127, 0, 0, 1), Conn: 1, Port: 27017, Opened: true},
		"waiting for connections":                     message.Listening{},
	}

	mongod := mongod{}
	for value, expected := range valid {
		r := internal.NewRuneReader(value)
		got, err := mongod.Network(*r)
		if err != nil {
			t.Errorf("network parse failed, got: %s", err)
		} else if !reflect.DeepEqual(expected, got) {
			t.Errorf("network mismatch, expected (%v), got (%v)", expected, got)
		}
	}

	invalid := []string{
		"connection accepted from 127.0.0.1",
		"connection accepted from :27017 #1",
		"connection accepted from #1",
	}

	for _, value := range invalid {
		r := internal.NewRuneReader(value)
		msg, err := mongod.Network(*r)
		if err == nil || msg != nil {
			t.Errorf("network should have failed on '%s' (%v)", value, msg)
		}
	}
}
