package format

import (
	"io"
	"strconv"

	"github.com/olekukonko/tablewriter"
)

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

func PrintQueryTable(patterns []PatternSummary, out io.Writer) {
	table := tablewriter.NewWriter(out)
	defer table.Render()

	if len(patterns) == 0 {
		out.Write([]byte("no queries found."))
		return
	}

	table.Append([]string{"namespace", "operation", "pattern", "count", "min (ms)", "max (ms)", "mean (ms)", "95%-ile (ms)", "sum (ms)"})
	table.SetBorder(false)
	table.SetRowLine(false)
	table.SetCenterSeparator(" ")
	table.SetColumnSeparator("  ")
	table.SetColWidth(60)

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
			table.Append([]string{
				pattern.Namespace,
				pattern.Operation,
				pattern.Pattern,
				strconv.FormatInt(pattern.Count, 10),
				strconv.FormatInt(pattern.Min, 10),
				strconv.FormatInt(pattern.Max, 10),
				strconv.FormatFloat(float64(pattern.Sum/pattern.Count), 'f', 1, 64),
				strconv.FormatFloat(pattern.N95Percentile, 'f', 1, 64),
				strconv.FormatInt(pattern.Sum, 10),
			})
		}
	}
}
