package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
	"mgotools/parser/record"
)

func mongodBuildInfo(r *internal.RuneReader) (message.Message, error) {
	return message.BuildInfo{BuildInfo: r.SkipWords(2).Remainder()}, nil
}

func mongodDbVersion(r *internal.RuneReader) (message.Message, error) {
	return makeVersion(r.SkipWords(2).Remainder(), "mongod")
}

func mongodJournal(r *internal.RuneReader) (message.Message, error) {
	path := r.Skip(12).Remainder()
	if path == "" {
		return nil, internal.UnexpectedEOL
	}

	// journal dir=
	return message.Journal(path), nil
}

func mongodOptions(r *internal.RuneReader) (message.Message, error) {
	r.SkipWords(1)
	return startupOptions(r.Remainder())
}

func mongodParseShutdown(r *internal.RuneReader) (message.Message, error) {
	return message.Shutdown{String: r.Remainder()}, nil
}

func mongodStartupInfo(entry record.Entry, r *internal.RuneReader) (message.Message, error) {
	return startupInfo(entry.RawMessage)
}
