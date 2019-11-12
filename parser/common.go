package parser

import (
	"bytes"
	"net"
	"strconv"
	"strings"

	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser/message"
	"mgotools/parser/record"
)

func commonParseAuthenticatedPrincipal(r *internal.RuneReader) (message.Message, error) {
	// Ignore the first four words and retrieve principal user
	r.SkipWords(4)
	user, ok := r.SlurpWord()
	if !ok {
		return nil, internal.UnexpectedEOL
	}

	// SERVER-39820
	ip, _ := r.SlurpWord()
	return message.Authentication{Principal: user, IP: ip}, nil
}

func commonParseBuildInfo(r *internal.RuneReader) (message.Message, error) {
	return message.BuildInfo{BuildInfo: r.SkipWords(2).Remainder()}, nil
}

func commonParseConnectionAccepted(r *internal.RuneReader) (message.Message, error) {
	if addr, port, conn, ok := connectionInit(r.SkipWords(3)); ok {
		return message.Connection{Address: addr, Port: port, Conn: conn, Opened: true}, nil
	}
	return nil, internal.NetworkUnrecognized
}

func commonParseClientMetadata(r *internal.RuneReader) (message.Message, error) {
	// Skip "received client metadata" and grab connection information.
	addr, port, conn, ok := connectionInit(r.SkipWords(4))
	if !ok {
		return nil, internal.MetadataUnmatched
	}

	meta, err := mongo.ParseJsonRunes(r, false)
	if err == nil {
		return nil, err
	}

	return message.ConnectionMeta{
		Connection: message.Connection{
			Address: addr,
			Conn:    conn,
			Port:    port,
			Opened:  true},
		Meta: meta}, nil
}

func commonParseConnectionEnded(entry record.Entry, r *internal.RuneReader) (message.Message, error) {
	if addr, port, ok := connectionTerminate(r.SkipWords(2)); ok {
		return message.Connection{Address: addr, Port: port, Conn: entry.Connection, Opened: false}, nil
	}
	return nil, internal.UnexpectedValue
}

func commonParseSignalProcessing(r *internal.RuneReader) (message.Message, error) {
	return message.Signal{String: r.String()}, nil
}

func commonParseWaitingForConnections(_ *internal.RuneReader) (message.Message, error) {
	return message.Listening{}, nil
}

func commonParseWiredtigerOpen(r *internal.RuneReader) (message.Message, error) {
	return message.WiredTigerConfig{String: r.SkipWords(2).Remainder()}, nil
}

func connectionInit(msg *internal.RuneReader) (ip net.IP, port uint16, conn int, success bool) {
	ip, port, success = parseAddress(msg)
	if !success {
		return
	}

	if msgNumber, ok := msg.SlurpWord(); ok {
		if msgNumber[0] == '#' {
			conn, _ = strconv.Atoi(msgNumber[1:])
		} else if len(msgNumber) > 4 && strings.HasPrefix(msgNumber, "conn") {
			if strings.HasSuffix(msgNumber, ":") {
				msgNumber = msgNumber[:len(msgNumber)-1]
			}
			conn, _ = strconv.Atoi(msgNumber[4:])
		}
	}

	return
}

func connectionTerminate(msg *internal.RuneReader) (ip net.IP, port uint16, success bool) {
	ip, port, success = parseAddress(msg)
	return
}

func makeVersion(msg string, binary string) (message.Version, error) {
	msg = strings.TrimLeft(msg, "v")
	version := message.Version{Binary: binary}

	if parts := strings.Split(msg, "."); len(parts) >= 2 {
		version.Major, _ = strconv.Atoi(parts[0])
		version.Minor, _ = strconv.Atoi(parts[1])

		if len(parts) >= 3 {
			version.Revision, _ = strconv.Atoi(parts[2])
		}
	}

	if msg == "" {
		return version, internal.UnexpectedVersionFormat
	}

	return version, nil
}

func parseAddress(r *internal.RuneReader) (ip net.IP, port uint16, ok bool) {
	addr, ok := r.SlurpWord()
	if !ok {
		return nil, 0, false
	}

	pos := bytes.IndexRune([]byte(addr), ':')
	if pos < 1 {
		return nil, 0, false

	}

	if value, err := strconv.Atoi(addr[pos+1:]); err != nil {
		return nil, 0, false
	} else {
		port = uint16(value)
	}
	if pos >= len(addr) {
		return nil, 0, false
	}

	ip = net.ParseIP(addr[:pos])
	ok = true
	return
}

func startupInfo(msg string) (message.StartupInfo, error) {
	if optionsRegex, err := internal.GetRegexRegistry().Compile(`([^=\s]+)=([^\s]+)`); err == nil {
		matches := optionsRegex.FindAllStringSubmatch(msg, -1)
		startupInfo := message.StartupInfo{}

		for _, match := range matches {
			switch match[1] {
			case "dbpath":
				startupInfo.DbPath = match[2]
			case "host":
				startupInfo.Hostname = match[2]
			case "pid":
				startupInfo.Pid, _ = strconv.Atoi(match[2])
			case "port":
				startupInfo.Port, _ = strconv.Atoi(match[2])
			}
		}
		return startupInfo, nil
	}
	return message.StartupInfo{}, internal.NoStartupArgumentsFound
}

func startupOptions(msg string) (message.StartupOptions, error) {
	opt, err := mongo.ParseJson(msg, false)
	if err != nil {
		return message.StartupOptions{}, err
	}
	return message.StartupOptions{String: msg, Options: opt}, nil
}
