// The connstats package provides statistics about connections found in a log.
//

package command

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"
	"time"

	"mgotools/command/format"
	"mgotools/parser"
	"mgotools/parser/context"
	"mgotools/record"
	"mgotools/util"
)

const minDuration = time.Duration(-1 << 63)
const maxDuration = time.Duration(1<<63 - 1)

func init() {
	args := Definition{
		Usage: "generate statistics about connections found in a log file",
		Flags: []Argument{
			{Name: "conn", Type: Bool, Usage: "per connection"},
			{Name: "ip", Type: Bool, Usage: "per IP address [default]"},
		},
	}

	GetFactory().Register("connstats", args, func() (command Command, err error) {
		c := &connstats{
			buffer:   bytes.NewBuffer([]byte{}),
			Instance: make(map[int]connstatsInstance),
		}

		return c, nil
	})
}

type connection struct {
	ID        int
	IP        string
	Opened    time.Time
	Closed    time.Time
	Exception bool
}

type connstatsInstance struct {
	summary     format.LogSummary
	connections map[int]*connection
}

type connstatsDuration struct {
	Min      time.Duration
	Max      time.Duration
	Duration time.Duration
	Total    int64

	Opened uint64
	Closed uint64
}

type connstats struct {
	Instance map[int]connstatsInstance

	buffer *bytes.Buffer

	conn bool
	ip   bool
}

func (c *connstats) Finish(index int, out commandTarget) error {
	instance := c.Instance[index]

	// Capture the file summary and output it to the buffer.
	instance.summary.Print(c.buffer)

	// Add some spacing and begin processing the connections.
	writer := bufio.NewWriter(c.buffer)
	writer.WriteRune('\n')

	var (
		opened uint64
		closed uint64
		full   uint64
		exceps uint64

		ips   = make(map[string]connstatsDuration)
		conns = make(map[int]time.Duration)

		overall connstatsDuration
	)

	// Reset the maximum and minimum.
	overall.Max = minDuration
	overall.Min = maxDuration

	get := func(ip string) connstatsDuration {
		if ip, ok := ips[ip]; ok {
			return ip
		} else {
			return connstatsDuration{Min: maxDuration, Max: minDuration}
		}
	}

	// Iterate through each connection and categorize them by connection ID
	// and IP address.
	for id, conn := range instance.connections {
		ip := get(conn.IP)
		var completed = false

		if !conn.Opened.IsZero() {
			opened += 1
			ip.Opened += 1
		}
		if !conn.Closed.IsZero() {
			closed += 1
			ip.Closed += 1
		}
		if conn.Exception {
			exceps += 1
		}
		if !conn.Opened.IsZero() && !conn.Closed.IsZero() {
			full += 1
			conns[id] = conn.Closed.Sub(conn.Opened)
			completed = true

			overall.Duration += conns[id]
			overall.Total += 1

			if conns[id] < overall.Min {
				overall.Min = conns[id]
			}
			if conns[id] > overall.Max {
				overall.Max = conns[id]
			}
		}
		if c.ip {
			ip.Duration += conns[id]

			if completed {
				ip.Total += 1

				if conns[id] < ip.Min {
					ip.Min = conns[id]
				}
				if conns[id] > ip.Max {
					ip.Max = conns[id]
				}
			}

			ips[conn.IP] = ip
		}
	}

	// Print an overview of connections statistics.
	c.printOverview(opened, closed, uint64(len(ips)), exceps)

	// Print an overview of connection aggregated statistics.
	c.printDurations(overall.Total, overall.Duration, overall.Min, overall.Max)
	c.buffer.WriteRune('\n')

	if c.conn {
		// Print each connection and associated statistics.
		c.printConn(instance.connections)
		c.buffer.WriteRune('\n')
	}

	if c.ip {
		// Print each unique IP address and associated statistics.
		c.printIP(ips)
		c.buffer.WriteRune('\n')
	}

	return nil
}

func (c *connstats) Prepare(name string, index int, args ArgumentCollection) error {
	c.Instance[index] = connstatsInstance{
		summary:     format.NewLogSummary(name),
		connections: make(map[int]*connection),
	}

	if args.Booleans["conn"] {
		c.conn = true
	}

	c.ip = true
	if !args.Booleans["ip"] {
		c.ip = false
	}

	return nil
}

func (c *connstats) Run(index int, _ commandTarget, in commandSource, error commandError) error {
	context := context.New(parser.VersionParserFactory.GetAll(), util.DefaultDateParser.Clone())
	defer context.Finish()

	type ConnectionBundle struct {
		Msg  record.MsgConnection
		Time time.Time
	}

	instance := c.Instance[index]
	summary := &instance.summary

	for base := range in {
		entry, err := context.NewEntry(base)
		if err != nil {
			continue
		}

		// Update the summary table with every entry.
		summary.Update(entry)

		// Ignore anything without a parsed message since only connections will
		// be examined.
		if entry.Message == nil {
			continue
		}

		conn, ok := entry.Message.(record.MsgConnection)
		if !ok && entry.DateValid {
			continue
		}

		ref, ok := instance.connections[conn.Conn]
		if conn.Opened {
			// A new connection opened so store a reference to a new connection
			// object and
			if ok {
				error <- fmt.Errorf("multiple new connections exist "+
					"for the same connection ID (%d)", conn.Conn)
			} else if !entry.DateValid {
				error <- fmt.Errorf("connection opened without valid "+
					"date format at line %d", entry.LineNumber)
			} else {
				ref = &connection{Opened: entry.Date, ID: conn.Conn}
				instance.connections[conn.Conn] = ref
			}
		} else {
			if !ok {
				// A connection closed without a connection open. This can be
				// caused by parsing a partial log file. Count it and continue
				// forward.
				ref = &connection{Closed: entry.Date, ID: conn.Conn}
				instance.connections[conn.Conn] = ref
			} else if !ref.Closed.IsZero() {
				// The connection was already closed? This shouldn't happen
				// unless multiple logs have been concatenated.
				error <- fmt.Errorf("duplicate closing found at line %d",
					entry.LineNumber)
			} else {
				// Record the close date. Theoretically, we're done with this
				// connection number from this point forward.
				ref.Closed = entry.Date
			}
		}

		// Record the IP address overriding any existing address (they should
		// always be the same).
		ref.IP = conn.Address.String()
		if conn.Exception != "" {
			// This may be discarded if _ref_ does not already exist in the map.
			ref.Exception = true
		}
	}

	return nil
}

func (c *connstats) Terminate(out commandTarget) error {
	out <- c.buffer.String()
	return nil
}

func (c connstats) printOverview(opened, closed, ips, exceptions uint64) {
	c.buffer.WriteString(fmt.Sprintf("     total opened: %d\n", opened))
	c.buffer.WriteString(fmt.Sprintf("     total closed: %d\n", closed))
	c.buffer.WriteString(fmt.Sprintf("    no unique IPs: %d\n", ips))
	c.buffer.WriteString(fmt.Sprintf("socket exceptions: %d\n", exceptions))
}

func (c connstats) printDurations(total int64, dur, min, max time.Duration) {
	c.buffer.WriteString(fmt.Sprintf("overall average connection duration(s): %.1fms\n", dur.Seconds()/float64(total)/1000))
	c.buffer.WriteString(fmt.Sprintf("overall minimum connection duration(s): %.1fms\n", min.Seconds()/1000))
	c.buffer.WriteString(fmt.Sprintf("overall maximum connection duration(s): %.1fms\n", max.Seconds()/1000))
}

func (c connstats) printConn(connections map[int]*connection) {
	i := 0
	keys := make([]int, len(connections))
	for conn := range connections {
		keys[i] = conn
		i += 1
	}

	sort.Ints(keys)

	for i = 0; i < len(keys); i += 1 {
		conn := connections[keys[i]]
		if conn.Opened.IsZero() && conn.Closed.IsZero() {
			// Ignore connections that aren't opened or closed (exceptions).
			continue
		} else if !conn.Opened.IsZero() && !conn.Closed.IsZero() {
			// A connection was opened and closed so we can provide a duration.
			c.buffer.WriteString(fmt.Sprintf("%-14d "+
				"opened: %-18s  "+
				"closed: %-18s  "+
				"dur(s): %8.2f\n",
				keys[i],
				conn.Opened.Format(string(util.DateFormatIso8602Utc)),
				conn.Closed.Format(string(util.DateFormatIso8602Utc)),
				conn.Closed.Sub(conn.Opened).Seconds(),
			))
		} else if conn.Opened.IsZero() {
			c.buffer.WriteString(fmt.Sprintf("%-14d "+
				"opened: n/a                       "+
				"closed: %-18s\n",
				keys[i],
				conn.Closed.Format(string(util.DateFormatIso8602Utc))))
		} else if conn.Closed.IsZero() {
			c.buffer.WriteString(fmt.Sprintf("%-14d "+
				"opened: %-18s  "+
				"closed: n/a\n",
				keys[i],
				conn.Opened.Format(string(util.DateFormatIso8602Utc))))
		}
	}
}

func (c connstats) printIP(ips map[string]connstatsDuration) {
	// Get a list of all IPs for printing.
	i := 0
	keys := make([]string, len(ips))
	for ip := range ips {
		keys[i] = ip
		i += 1
	}

	// Sort the key list before displaying.
	sort.Strings(keys)

	for i = 0; i < len(keys); i += 1 {
		dur := ips[keys[i]]
		var avg = time.Duration(0)
		if dur.Total > 0 {
			avg = dur.Duration / time.Duration(dur.Total)
		}

		c.buffer.WriteString(fmt.Sprintf(
			"%-14s opened: %8d  "+
				"closed: %8d  ",
			keys[i], dur.Opened,
			dur.Closed,
		))

		if dur.Min != maxDuration && dur.Max != minDuration {
			c.buffer.WriteString(fmt.Sprintf(
				"dur-avg: %8.2f  "+
					"dur-min(s): %8.2f  "+
					"dur-max(s): %8.2f\n",
				avg.Seconds(),
				dur.Min.Seconds(),
				dur.Max.Seconds()))
		} else {
			c.buffer.WriteRune('\n')
		}
	}
}
