package parser

import (
	"strings"

	"mgotools/parser/format"
	"mgotools/record"
	"mgotools/util"
)

type VersionSCommon struct {
	VersionBaseParser
}

var logVersionSCommon = VersionSCommon{}

func (v *VersionSCommon) NewLogMessage(entry record.Entry, err error) (record.Message, error) {
	msg := util.NewRuneReader(entry.RawMessage)
	preview1 := strings.TrimRight(msg.PreviewWord(1), ":")
	preview2 := strings.TrimRight(msg.PreviewWord(2), ":")

	switch entry.Context {
	case "mongosMain":
		switch {
		case preview2 == "MongoS version":
			return format.StartupInfo(entry.RawMessage)
		case preview2 == "db version":
			return format.Version(msg.SkipWords(2).Remainder(), "mongos")
		case preview2 == "OpenSSL version":
			return format.Version(msg.SkipWords(2).Remainder(), "OpenSSL")
		case preview1 == "options":
			return format.StartupOptions(msg.SkipWords(1).Remainder())
		}
	}
	return nil, err
}
