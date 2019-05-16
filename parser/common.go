package parser

import (
	"bytes"
	"net"
	"strconv"
	"strings"

	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/parser/message"
)

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
