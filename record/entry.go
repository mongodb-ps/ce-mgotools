package record

import (
	"bytes"
	"net"
	"time"

	"mgotools/util"
)

// Log examples:
// Sat Jul 29 16:51:28.392 [initandlisten] db version v2.4.14
// 2017-07-29T16:53:40.671-0700 [initandlisten] db version v2.6.12
// 2017-07-29T16:55:33.242-0700 I CONTROL  [initandlisten] db version v3.0.15
// 2017-07-29T17:01:15.835-0700 I CONTROL  [initandlisten] db version v3.2.12

type Entry struct {
	Base
	Message Message

	Connection      int
	Context         string
	Date            time.Time
	DateYearMissing bool
	DateRollover    int
	DateValid       bool
	Thread          int

	Valid bool
}

type EntryEventConnectionAttributes struct {
	Address net.IPAddr
	Port    uint16
}

func (r *Entry) String() string {
	var buffer bytes.Buffer
	buffer.WriteString(r.Date.Format(util.DATE_FORMAT_ISO8602_LOCAL))
	buffer.WriteString(" ")
	buffer.WriteString(r.RawSeverity)
	buffer.WriteString(" ")
	buffer.WriteString(r.RawComponent)
	buffer.WriteString("  ")
	buffer.WriteString(r.RawContext)
	buffer.WriteString(" ")
	buffer.WriteString(r.RawMessage)
	return buffer.String()
}
