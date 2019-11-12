package parser

import (
	"mgotools/internal"
	"mgotools/parser/message"
)

func mongosParseStartupOptions(r *internal.RuneReader) (message.Message, error) {
	return startupOptions(r.SkipWords(1).Remainder())
}

func mongosParseVersion(r *internal.RuneReader) (message.Message, error) {
	versionString, ok := r.SkipWords(2).SlurpWord()
	if !ok {
		return nil, internal.UnexpectedVersionFormat
	}

	if version, err := makeVersion(versionString, "mongos"); err != nil {
		return nil, err
	} else if err == nil && !r.ExpectString("starting:") {
		return version, nil
	} else if info, err := startupInfo(r.Remainder()); err != nil {
		return nil, err
	} else {
		return message.StartupInfoLegacy{
			Version:     version,
			StartupInfo: info,
		}, nil
	}
}
