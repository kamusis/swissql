package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/olekukonko/tablewriter"
	tw "github.com/olekukonko/tablewriter/tw"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

// handleReplTopCommands handles top-related commands such as \top.
func handleReplTopCommands(
	_ *cobra.Command,
	line *liner.State,
	historyMode string,
	input string,
	cmdName string,
	_ []string,
	c *client.Client,
	sessionId string,
	_ *config.Config,
) bool {
	cmdLower := strings.ToLower(cmdName)

	switch {
	case cmdLower == "\\top":
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		return handleTopCommand(c, sessionId)
	}

	return false
}

// handleTopCommand executes the \top command to display top performance metrics.
func handleTopCommand(c *client.Client, sessionId string) bool {
	snapshot, err := c.SamplerSnapshot(sessionId, "top")
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return true
	}

	renderCollectorResult(getOutputWriter(), snapshot)
	return true
}

// renderCollectorResult renders collector layers dynamically, using order + render_hint.
func renderCollectorResult(w io.Writer, snapshot *client.CollectorResult) {
	if snapshot == nil {
		fmt.Fprintln(w, "Error: empty snapshot")
		return
	}

	fmt.Fprintf(w, "Collector: %s", snapshot.CollectorId)
	if strings.TrimSpace(snapshot.SourceFile) != "" {
		fmt.Fprintf(w, " (%s)", snapshot.SourceFile)
	}
	if strings.TrimSpace(snapshot.DbType) != "" {
		fmt.Fprintf(w, " db_type=%s", snapshot.DbType)
	}
	fmt.Fprintln(w)

	if snapshot.Layers == nil || len(snapshot.Layers) == 0 {
		fmt.Fprintln(w, "(no layers)")
		return
	}

	type layerEntry struct {
		id    string
		order int
		layer client.LayerResult
	}

	items := make([]layerEntry, 0, len(snapshot.Layers))
	for id, layer := range snapshot.Layers {
		items = append(items, layerEntry{id: id, order: layer.Order, layer: layer})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].order != items[j].order {
			return items[i].order < items[j].order
		}
		return items[i].id < items[j].id
	})

	for _, it := range items {
		title := it.id
		displayType := "table"
		preferredColumns := []string(nil)
		if it.layer.RenderHint != nil {
			if v, ok := it.layer.RenderHint["title"].(string); ok && strings.TrimSpace(v) != "" {
				title = v
			}
			if v, ok := it.layer.RenderHint["display_type"].(string); ok && strings.TrimSpace(v) != "" {
				displayType = strings.ToLower(strings.TrimSpace(v))
			}
			if v, ok := it.layer.RenderHint["preferred_columns"]; ok {
				switch vv := v.(type) {
				case []interface{}:
					for _, col := range vv {
						if s, ok := col.(string); ok {
							preferredColumns = append(preferredColumns, s)
						}
					}
				case []string:
					preferredColumns = append(preferredColumns, vv...)
				}
			}
		}

		rows := it.layer.Rows
		fmt.Fprintf(w, "\n%s:\n", title)
		if displayType == "summary_line" && len(rows) > 0 {
			// Minimal summary renderer: print all fields of first row as k=v.
			first := rows[0]
			keys := make([]string, 0, len(first))
			for k := range first {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				fmt.Fprintf(w, "  %s=%s", k, formatValue(first[k]))
			}
			fmt.Fprintln(w)
			continue
		}

		renderDynamicTable(w, rowsToOrderedMaps(rows), preferredColumns)
	}
}

func rowsToOrderedMaps(rows []map[string]any) []client.OrderedMap {
	out := make([]client.OrderedMap, 0, len(rows))
	for _, r := range rows {
		m := client.OrderedMap{Values: map[string]any{}}
		keys := make([]string, 0, len(r))
		for k := range r {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			m.Set(k, r[k])
		}
		out = append(out, m)
	}
	return out
}

// formatValue formats a value for display, handling various numeric types.
func formatValue(v interface{}) string {
	switch val := v.(type) {
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case int32:
		return fmt.Sprintf("%d", val)
	case float64:
		// Display as integer if it's a whole number
		if val == float64(int64(val)) {
			return fmt.Sprintf("%.0f", val)
		}
		return fmt.Sprintf("%.2f", val)
	case float32:
		if val == float32(int32(val)) {
			return fmt.Sprintf("%.0f", val)
		}
		return fmt.Sprintf("%.2f", val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// renderDynamicTable renders a table dynamically based on available columns.
func renderDynamicTable(w io.Writer, rows []client.OrderedMap, preferredColumns []string) {
	if len(rows) == 0 {
		return
	}

	// Determine available columns
	var availableColumns []string
	if preferredColumns != nil {
		// Use preferred columns that exist in data
		for _, col := range preferredColumns {
			if _, exists := rows[0].Get(col); exists {
				availableColumns = append(availableColumns, col)
			}
		}
	} else {
		// Use key order from the first row
		availableColumns = append(availableColumns, rows[0].Keys...)
	}

	if len(availableColumns) == 0 {
		return
	}

	switch outputFormat {
	case "json":
		b, err := json.Marshal(rows)
		if err != nil {
			fmt.Fprintf(w, "%v\n", err)
			return
		}
		fmt.Fprintf(w, "%s\n", string(b))
		return
	case "csv":
		renderDynamicDelimited(w, availableColumns, rows, ',')
		return
	case "tsv":
		renderDynamicDelimited(w, availableColumns, rows, '\t')
		return
	default:
		renderDynamicTableWriter(w, availableColumns, rows)
		return
	}
}

func renderDynamicDelimited(w io.Writer, columns []string, rows []client.OrderedMap, comma rune) {
	csvWriter := csv.NewWriter(w)
	csvWriter.Comma = comma
	_ = csvWriter.Write(columns)
	for _, row := range rows {
		values := make([]string, 0, len(columns))
		for _, col := range columns {
			v, _ := row.Get(col)
			values = append(values, fmt.Sprintf("%v", v))
		}
		_ = csvWriter.Write(values)
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		fmt.Fprintf(w, "%v\n", err)
		return
	}
}

func renderDynamicTableWriter(w io.Writer, columns []string, rows []client.OrderedMap) {
	table := tablewriter.NewWriter(w)
	table.Options(tablewriter.WithConfig(tablewriter.Config{
		Header: tw.CellConfig{
			Formatting: tw.CellFormatting{AutoFormat: tw.Off},
		},
	}))

	// On Windows terminals, Unicode box-drawing can render poorly.
	// Only force ASCII when writing to an interactive stdout (not when redirected).
	if runtime.GOOS == "windows" && shouldPageOutput(w) {
		table.Options(tablewriter.WithSymbols(&tw.SymbolASCII{}))
	}

	headers := make([]any, 0, len(columns))
	for _, col := range columns {
		headers = append(headers, col)
	}
	table.Header(headers...)

	for _, row := range rows {
		values := make([]any, 0, len(columns))
		for _, col := range columns {
			v, _ := row.Get(col)
			values = append(values, fmt.Sprintf("%v", v))
		}
		table.Append(values...)
	}

	table.Render()
}
