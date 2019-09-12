package record

import (
	"bytes"
	"time"

	"mgotools/internal"
	"mgotools/parser/message"
)

// Log examples:
// Sat Jul 29 16:51:28.392 [initandlisten] db version v2.4.14
// 2017-07-29T16:53:40.671-0700 [initandlisten] db version v2.6.12
// 2017-07-29T16:55:33.242-0700 I CONTROL  [initandlisten] db version v3.0.15
// 2017-07-29T17:01:15.835-0700 I CONTROL  [initandlisten] db version v3.2.12

type Entry struct {
	Base
	Message message.Message

	Connection      int
	Context         string
	Date            time.Time
	Format          internal.DateFormat
	DateYearMissing bool
	DateRollover    int
	DateValid       bool
	Thread          int

	Valid bool
}

func (r *Entry) String() string {
	var buffer = bytes.NewBuffer(make([]byte, 512))
	if r.Format != "" {
		buffer.WriteString(string(r.Format))
	} else {
		buffer.WriteString(string(internal.DateFormatIso8602Utc))
	}

	buffer.WriteString(" ")
	buffer.WriteString(r.Severity.String())
	buffer.WriteString(" ")
	buffer.WriteString(r.Component.String())
	buffer.WriteString("  ")
	buffer.WriteString(r.RawContext)
	buffer.WriteString(" ")
	buffer.WriteString(r.RawMessage)
	return buffer.String()
}
