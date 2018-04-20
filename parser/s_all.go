package parser

import (
	"mgotools/log"
	"mgotools/util"
	"strings"
)

type LogVersionSCommon struct {
	LogVersionCommon
}

var logVersionSCommon = LogVersionSCommon{}

func (v *LogVersionSCommon) NewLogMessage(entry log.Entry) (log.Message, error) {
	msg := util.NewRuneReader(entry.RawMessage)
	preview1 := strings.TrimRight(msg.PreviewWord(1), ":")
	preview2 := strings.TrimRight(msg.PreviewWord(2), ":")

	switch entry.Context {
	case "mongosMain":
		switch {
		case preview2 == "MongoS version":
			return parseStartupInfo(entry.RawMessage)
		case preview2 == "db version":
			return parseVersion(msg.SkipWords(2).Remainder(), "mongos")
		case preview2 == "OpenSSL version":
			return parseVersion(msg.SkipWords(2).Remainder(), "OpenSSL")
		case preview1 == "options":
			return parseStartupOptions(msg.SkipWords(1).Remainder())
		}
	}
	return nil, LogVersionErrorUnmatched{Message: "version (s)all"}
}
