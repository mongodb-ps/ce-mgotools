package parser

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"mgotools/mongo"
	"mgotools/record"
	"mgotools/util"

	"github.com/pkg/errors"
)

var ErrorNoPlanSummaryFound = errors.New("no plan summary found")
var ErrorNoStartupArgumentsFound = errors.New("no startup arguments found")
var ErrorUnexpectedVersionFormat = errors.New("unexpected version format")
var ErrorNetworkUnrecognized = VersionErrorUnmatched{"unrecognized network message"}
var ErrorControlUnrecognized = VersionErrorUnmatched{Message: "unrecognized control message"}
var ErrorDDLUnrecognized = VersionErrorUnmatched{Message: "unrecognized ddl message"}
var ErrorMetadataUnmatched = VersionErrorUnmatched{"unexpected connection meta format"}
var ErrorStorageUnmatched = VersionErrorUnmatched{"unrecognized storage option"}
var ErrorComponentUnmatched = errors.New("component unmatched")

func NormalizeCommand(msg record.Message) (record.Message) {
	cmd, ok := record.MsgOpCommandBaseFromMessage(msg)
	if !ok {
		return cmd
	}

	switch cmd.Command {
	case "count",
		"find",
		"getmore",
		"geonear",
		"remove",
		"distinct":
		if filter, ok := NormalizeQuery(*cmd); ok {
			cmd.Namespace = cmd.Payload[cmd.Command].(string)
			cmd.Payload = filter
		}

	case "aggregate",
		"explain",
		"insert",
		"update":
		cmd.Command = cmd.Operation

	case "create":
		if name, ok := cmd.Payload["create"].(string); ok {
			return record.MsgCreateCollection{*cmd, name}
		}

	case "createIndexes":
		if ns, ok := cmd.Payload["createIndexes"].(string); ok {
			return record.MsgOpIndex{
				MsgOperation: record.MsgOperation{
					Namespace: ns,
					Operation:
					cmd.Command},
				Properties: cmd.Payload["indexes"].(map[string]interface{})}
		}

	case "isMaster":
	default:
		util.Debug("%#v", cmd)
		panic(fmt.Sprintf("unexpected %s in query operation", cmd.Command))
	}

	return msg
}

func NormalizeQuery(cmd record.MsgOpCommandBase) (record.Filter, bool) {
	convert := func(m interface{}) record.Filter {
		if m != nil {
			if n, ok := m.(record.Filter); ok {
				return n
			}
		}
		return record.Filter{}
	}
	switch cmd.Command {
	case "count", "distinct":
		q, ok := cmd.Payload["query"]
		return convert(q), ok
	case "find", "getmore", "getMore":
		q, ok := cmd.Payload["filter"]
		return convert(q), ok
	case "geonear", "geoNear":
		q, ok := cmd.Payload["query"]
		c := convert(q)
		if ok && c != nil {
			c["near"], ok = cmd.Payload["near"]
		}
		return c, ok
	default:
		panic(fmt.Sprintf("unrecognzied query type during normalization: %s", cmd.Command))
	}
}

func ParseControl(r util.RuneReader, entry record.Entry) (record.Message, error) {
	switch entry.Context {
	case "initandlisten":
		switch {
		case r.ExpectString("build info"):
			return record.MsgBuildInfo{BuildInfo: r.SkipWords(2).Remainder()}, nil
		case r.ExpectString("db version"):
			return parseVersion(r.SkipWords(2).Remainder(), "mongod")
		case r.ExpectString("MongoDB starting"):
			return parseStartupInfo(entry.RawMessage)
		case r.ExpectString("OpenSSL version"):
			return parseVersion(r.SkipWords(2).Remainder(), "OpenSSL")
		case r.ExpectString("options"):
			r.SkipWords(1)
			return parseStartupOptions(r.Remainder())
		}
	case "signalProcessingThread":
		if r.ExpectString("dbexit") {
			return record.MsgShutdown{String: r.Remainder()}, nil
		} else {
			return record.MsgSignal{r.Remainder()}, nil
		}
	}
	return nil, ErrorControlUnrecognized
}

func ParseDDL(r util.RuneReader, entry record.Entry) (record.Message, error) {
	if entry.Connection > 0 {
		switch {
		case r.ExpectString("CMD: drop"):
			if namespace, ok := r.SkipWords(2).SlurpWord(); ok {
				return record.MsgDropCollection{namespace}, nil
			}
		case r.ExpectString("dropDatabase"):
			if database, ok := r.SkipWords(1).SlurpWord(); ok {
				if r.NextRune() == '-' {
					r.SkipWords(1)
				}
				return record.MsgDropDatabase{database, r.Remainder()}, nil
			}
		}
	}

	return nil, ErrorDDLUnrecognized
}

func ParseNetwork(r util.RuneReader, entry record.Entry, ) (record.Message, error) {
	if entry.Connection == 0 {
		if r.ExpectString("connection accepted") { // connection accepted from <IP>:<PORT> #<CONN>
			if addr, port, conn, ok := parseConnectionInit(r.SkipWords(2)); ok {
				return record.MsgConnection{Address: addr, Port: port, Conn: conn, Opened: true}, nil
			}
		} else if r.ExpectString("waiting for connections") {
			return record.MsgListening{}, nil
		} else if entry.Context == "signalProcessingThread" {
			return record.MsgSignal{entry.RawMessage}, nil
		}
	} else if entry.Connection > 0 {
		if r.ExpectString("end connection") {
			if addr, port, _, ok := parseConnectionInit(&r); ok {
				return record.MsgConnection{Address: addr, Port: port, Conn: entry.Connection, Opened: false}, nil
			}
		} else if r.ExpectString("received client metadata from") {
			// Skip "received client metadata" and grab connection information.
			if addr, port, conn, ok := parseConnectionInit(r.SkipWords(3)); !ok {
				return nil, ErrorMetadataUnmatched
			} else {
				if meta, err := mongo.ParseJsonRunes(&r, false); err == nil {
					return record.MsgConnectionMeta{
						MsgConnection: record.MsgConnection{
							Address: addr,
							Conn:    conn,
							Port:    port,
							Opened:  true},
						Meta: meta}, nil
				}
			}
		}
	}

	return nil, ErrorNetworkUnrecognized
}

func ParseStorage(r util.RuneReader, entry record.Entry) (record.Message, error) {
	switch entry.Context {
	case "signalProcessingThread":
		return record.MsgSignal{entry.RawMessage}, nil
	case "initandlisten":
		if r.ExpectString("wiredtiger_open config") {
			return record.MsgWiredTigerConfig{String: r.SkipWords(2).Remainder()}, nil
		}
	}

	return nil, ErrorStorageUnmatched
}

func parseConnectionInit(msg *util.RuneReader) (net.IP, uint16, int, bool) {
	var (
		addr   *util.RuneReader
		buffer string
		char   rune
		conn          = 0
		port          = 0
		ip     net.IP = nil
		ok     bool
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
	if buffer, ok = addr.Substr(pos, length-pos); ok {
		port, _ = strconv.Atoi(buffer)
	}
	if part, ok := addr.Substr(0, pos-1); ok {
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

func parsePlanSummary(r *util.RuneReader) ([]record.MsgOpCommandPlanSummary, error) {
	var out []record.MsgOpCommandPlanSummary
	for {
		if op, ok := r.SlurpWord(); !ok {
			// There are no words, so exit.
			break
		} else if r.NextRune() == '{' {
			if summary, err := mongo.ParseJsonRunes(r, false); err != nil {
				// The plan summary did not parse as valid JSON so exit.
				return nil, err
			} else {
				// The plan summary parsed as valid JSON, so record the operation and fall-through.
				out = append(out, record.MsgOpCommandPlanSummary{op, summary})
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
			out = append(out, record.MsgOpCommandPlanSummary{op[:length-1], nil})
			continue
		} else {
			// Finally, the plan summary is boring and only includes a single word (e.g. COLLSCAN).
			out = append(out, record.MsgOpCommandPlanSummary{op, nil})
			break
		}
	}
	if len(out) == 0 {
		// Return an error if no plans exist.
		return nil, ErrorNoPlanSummaryFound
	}
	return out, nil
}

func parseStartupInfo(msg string) (record.MsgStartupInfo, error) {
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
	return record.MsgStartupInfo{}, ErrorNoStartupArgumentsFound
}

func parseStartupOptions(msg string) (record.MsgStartupOptions, error) {
	opt, err := mongo.ParseJson(msg, false)
	if err != nil {
		return record.MsgStartupOptions{}, err
	}
	return record.MsgStartupOptions{String: msg, Options: opt}, nil
}

func parseVersion(msg string, binary string) (record.MsgVersion, error) {
	msg = strings.TrimLeft(msg, "v")
	version := record.MsgVersion{String: msg, Binary: binary}
	if parts := strings.Split(version.String, "."); len(parts) >= 2 {
		version.Major, _ = strconv.Atoi(parts[0])
		version.Minor, _ = strconv.Atoi(parts[1])
		if len(parts) >= 3 {
			version.Revision, _ = strconv.Atoi(parts[2])
		}
	}
	if version.String == "" {
		return version, ErrorUnexpectedVersionFormat
	}
	return version, nil
}
