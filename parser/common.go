package parser

import (
	"bytes"
	"net"
	"strconv"
	"strings"

	"mgotools/internal"
	"mgotools/mongo"
	"mgotools/record"
	"mgotools/util"
)

func connectionInit(msg *util.RuneReader) (ip net.IP, port uint16, conn int, success bool) {
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

func connectionTerminate(msg *util.RuneReader) (ip net.IP, port uint16, success bool) {
	ip, port, success = parseAddress(msg)
	return
}

func parseAddress(r *util.RuneReader) (ip net.IP, port uint16, ok bool) {
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

func startupInfo(msg string) (record.MsgStartupInfo, error) {
	if optionsRegex, err := util.GetRegexRegistry().Compile(`([^=\s]+)=([^\s]+)`); err == nil {
		matches := optionsRegex.FindAllStringSubmatch(msg, -1)
		startupInfo := record.MsgStartupInfo{}

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
	return record.MsgStartupInfo{}, internal.NoStartupArgumentsFound
}

func startupOptions(msg string) (record.MsgStartupOptions, error) {
	opt, err := mongo.ParseJson(msg, false)
	if err != nil {
		return record.MsgStartupOptions{}, err
	}
	return record.MsgStartupOptions{String: msg, Options: opt}, nil
}

func version(msg string, binary string) (record.MsgVersion, error) {
	msg = strings.TrimLeft(msg, "v")
	version := record.MsgVersion{Binary: binary}

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
