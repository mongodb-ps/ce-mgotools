package parser

import (
	"fmt"
	"mgotools/util"
	"net"
	"strconv"
	"strings"
	"unicode"
)

func parseConnectionInit(msg *util.RuneReader) (net.IP, uint16, int, bool) {
	var (
		addr   *util.RuneReader
		buffer string
		char   rune
		conn   int = 0
		ip     net.IP = nil
		ok     bool
		port   int = 0
	)
	msg.SkipWords(1) // "from"
	partialAddress, ok := msg.SlurpWord()
	if !ok {
		return nil, 0, 0, false
	}
	addr = util.NewRuneReader(partialAddress)
	length := addr.Length()
	addr.Seek(length, 0)
	for {
		if char, ok = addr.Prev(); !ok || char == ':' {
			addr.Next()
			break
		}
	}
	pos := addr.Pos()
	if buffer, ok = addr.Read(pos, length-pos); ok {
		port, _ = strconv.Atoi(buffer)
	}
	if part, ok := addr.Read(0, pos-1); ok {
		ip = net.ParseIP(part)
	}
	if msgNumber, ok := msg.SlurpWord(); ok && msgNumber[0] == '#' {
		conn, _ = strconv.Atoi(msgNumber[1:])
	}
	return ip, uint16(port), conn, true
}

func parseCommandPrefix(r *util.RuneReader) ([]string, bool) {
	if !r.ExpectString("command") {
		return nil, false
	} else if ns, ok := r.SlurpWord(); !ok {
		return nil, false
	} else if cmd, ok := r.SlurpWord(); !ok {
		return nil, false
	} else {
		return []string{"command", ns, strings.TrimRight(cmd, ":")}, true
	}
}

func parseIntegerKeyValue(source string, target map[string]int, limit map[string]string) bool {
	if key, num, ok := util.StringDoubleSplit(source, ':'); ok {
		if _, ok := limit[key]; ok {
			if count, err := strconv.ParseInt(num, 10, 32); err == nil {
				target[key] = int(count)
				return true
			}
		} else {
			panic("unexpected key " + key + " found")
		}
	}
	return false
}

func parseOperationExtended(r *util.RuneReader) (string, error) {
	for c, ok := r.Next(); ok; c, ok = r.Next() {
		switch c {
		case '"':
			if _, err := r.QuotedString(); err != nil {
				return "", err
			}
		case '{':
			if _, err := util.ParseJsonRunes(r, false); err != nil {
				return "", err
			}
		case ':':
			if p, ok := r.Prev(); ok && unicode.IsLetter(p) {
				for ; ok && !unicode.IsSpace(p); p, ok = r.Prev() {
				}
				return r.CurrentWord(), nil
			}
		case ',':
			return r.CurrentWord(), nil
		}
	}
	return r.CurrentWord(), nil
}

func parseStartupInfo(msg string) (LogMsgStartupInfo, error) {
	if optionsRegex, err := util.GetRegexRegistry().Compile(`([^=\s]+)=([^\s]+)`); err == nil {
		matches := optionsRegex.FindAllStringSubmatch(msg, -1)
		startupInfo := LogMsgStartupInfo{}

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
	return LogMsgStartupInfo{}, fmt.Errorf("no startup arguments found")
}

func parseStartupOptions(msg string) (LogMsgStartupOptions, error) {
	opt, err := util.ParseJson(msg, false)
	if err != nil {
		return LogMsgStartupOptions{}, err
	}
	return LogMsgStartupOptions{String: msg, Options: opt}, nil
}

func parseVersion(msg string, binary string) (LogMsgVersion, error) {
	msg = strings.TrimLeft(msg, "v")
	version := LogMsgVersion{String: msg, Binary: binary}
	if parts := strings.Split(version.String, "."); len(parts) >= 2 {
		version.Major, _ = strconv.Atoi(parts[0])
		version.Minor, _ = strconv.Atoi(parts[1])
		if len(parts) >= 3 {
			version.Revision, _ = strconv.Atoi(parts[2])
		}
	}
	if version.String == "" || version.Major == 0 {
		return version, fmt.Errorf("unexpected version format")
	}
	return version, nil
}
