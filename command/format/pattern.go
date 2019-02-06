package format

import (
	"io"
	"math"
	"strconv"

	"github.com/olekukonko/tablewriter"
)

type PatternTable []PatternSummary

type PatternSummary struct {
	Namespace     string
	Pattern       string
	Operation     string
	Count         int64
	Min           int64
	Max           int64
	N95Percentile float64
	Sum           int64
}

func (patterns PatternTable) Print(wrap bool, out io.Writer) {
	if len(patterns) == 0 {
		out.Write([]byte("no queries found."))
		return
	}

	table := tablewriter.NewWriter(out)
	defer table.Render()

	table.Append([]string{"namespace", "operation", "pattern", "count", "min (ms)", "max (ms)", "mean (ms)", "95%-ile (ms)", "sum (ms)"})
	table.SetBorder(false)
	table.SetRowLine(false)
	table.SetCenterSeparator(" ")
	table.SetColumnSeparator(" ")
	table.SetColWidth(60)
	table.SetAutoWrapText(wrap)

	for _, pattern := range patterns {
		if pattern.Count == 0 {
			table.Append([]string{
				pattern.Namespace,
				pattern.Operation,
				pattern.Pattern,
				"0",
				"-",
				"-",
				"-",
				"-",
				"-",
			})
		} else {
			var n95 = "-"
			if !math.IsNaN(pattern.N95Percentile) {
				n95 = strconv.FormatFloat(pattern.N95Percentile, 'f', 1, 64)
			}

			table.Append([]string{
				pattern.Namespace,
				pattern.Operation,
				pattern.Pattern,
				strconv.FormatInt(pattern.Count, 10),
				strconv.FormatInt(pattern.Min, 10),
				strconv.FormatInt(pattern.Max, 10),
				strconv.FormatFloat(float64(pattern.Sum/pattern.Count), 'f', 0, 64),
				n95,
				strconv.FormatInt(pattern.Sum, 10),
			})
		}
	}
}
