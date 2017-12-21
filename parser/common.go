package parser

import (
	"fmt"
	"github.com/pkg/errors"
	"mgotools/mongo"
	"mgotools/util"
	"net"
	"strconv"
	"strings"
)

// IsComponent checks a string value against the possible components array.
func IsComponent(value string) bool {
	return util.ArrayMatchString(mongo.COMPONENTS, value)
}

// IsContext checks for a bracketed string ([<string>])
func IsContext(value string) bool {
	length := util.StringLength(value)
	return length > 2 && value[0] == '[' && value[length-1] == ']'
}

// IsSeverity checks a string value against the severities array.
func IsSeverity(value string) bool {
	return util.StringLength(value) == 1 && util.ArrayMatchString(mongo.SEVERITIES, value)
}

func parseConnectionInit(msg *util.RuneReader) (net.IP, uint16, int, bool) {
	var (
		addr   *util.RuneReader
		buffer string
		char   rune
		conn   int    = 0
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
	return ip, uint16(port), conn, true
}

func parseIntegerKeyValue(source string, target map[string]int, limit map[string]string) bool {
	if key, num, ok := util.StringDoubleSplit(source, ':'); ok && num != "" {
		if _, ok := limit[key]; ok {
			if count, err := strconv.ParseInt(num, 10, 64); err == nil {
				target[key] = int(count)
				return true
			} else {
				panic(err)
			}
		}
	}
	return false
}

func parsePlanSummary(r *util.RuneReader) ([]LogMsgOpCommandPlanSummary, error) {
	var out []LogMsgOpCommandPlanSummary
	for {
		if op, ok := r.SlurpWord(); !ok {
			// There are no words, so exit.
			break
		} else if r.NextRune() == '{' {
			if summary, err := util.ParseJsonRunes(r, false); err != nil {
				// The plan summary did not parse as valid JSON so exit.
				return nil, err
			} else {
				// The plan summary parsed as valid JSON, so record the operation and fall-through.
				out = append(out, LogMsgOpCommandPlanSummary{op, summary})
			}
			if r.NextRune() != ',' {
				// There are no other plans so exit plan summary parsing.
				break
			} else {
				// There are more plans, so continue to run by repeating the for loop.
				r.Next()
				continue
			}
		} else if length := len(op); length > 2 && op[length-1] == ',' {
			// This is needed for repeated bare words (e.g. planSummary: COLLSCAN, COLLSCAN).
			out = append(out, LogMsgOpCommandPlanSummary{op[:length-1], nil})
			continue
		} else {
			// Finally, the plan summary is boring and only includes a single word (e.g. COLLSCAN).
			out = append(out, LogMsgOpCommandPlanSummary{op, nil})
			break
		}
	}
	if len(out) == 0 {
		// Return an error if no plans exist.
		return nil, errors.New("no plan summary found")
	}
	return out, nil
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
	if version.String == "" {
		return version, fmt.Errorf("unexpected version format")
	}
	return version, nil
}
