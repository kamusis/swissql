package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
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
	snapshot, err := c.GetTopSnapshot(sessionId)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return true
	}

	renderTopSnapshot(getOutputWriter(), snapshot)
	return true
}

// renderTopSnapshot renders the top snapshot with hide-when-missing logic.
func renderTopSnapshot(w io.Writer, snapshot *client.TopSnapshot) {
	// L0: Context header (dynamic fields)
	fmt.Fprintf(w, "Database: %s", snapshot.DbType)
	if snapshot.Context.Values != nil {
		if dbVersion := getOrderedMapField(snapshot.Context, "dbVersion"); dbVersion != nil {
			fmt.Fprintf(w, " (%s)", formatValue(dbVersion))
		}
		fmt.Fprintln(w)
		// Render all context fields dynamically
		for _, key := range snapshot.Context.Keys {
			if key == "dbVersion" {
				continue
			}
			if value, ok := snapshot.Context.Get(key); ok {
				fmt.Fprintf(w, "  %s: %s\n", key, formatValue(value))
			}
		}
	} else {
		fmt.Fprintln(w)
	}

	// L1: CPU metrics (dynamic fields)
	if snapshot.Cpu.Values != nil {
		fmt.Fprintf(w, "CPU:")
		for _, key := range snapshot.Cpu.Keys {
			if value, ok := snapshot.Cpu.Get(key); ok {
				fmt.Fprintf(w, " %s=%s", key, formatValue(value))
			}
		}
		fmt.Fprintln(w)
	}

	// L1: Session metrics (dynamic fields)
	if snapshot.Sessions.Values != nil {
		fmt.Fprintf(w, "Sessions:")
		for _, key := range snapshot.Sessions.Keys {
			if value, ok := snapshot.Sessions.Get(key); ok {
				fmt.Fprintf(w, " %s=%s", key, formatValue(value))
			}
		}
		fmt.Fprintln(w)
	}

	// L2: Wait events (dynamic list of maps)
	if len(snapshot.Waits) > 0 {
		fmt.Fprintf(w, "\nTop Wait Events:\n")
		renderDynamicTable(w, snapshot.Waits, nil) // nil = auto-detect all columns
	}

	// L3: Top sessions (dynamic list of maps)
	if len(snapshot.TopSessions) > 0 {
		fmt.Fprintf(w, "\nTop Sessions:\n")
		renderDynamicTable(w, snapshot.TopSessions, nil) // nil = auto-detect all columns
	}

	// L1: I/O metrics (dynamic fields)
	if snapshot.Io.Values != nil {
		fmt.Fprintf(w, "\nI/O:")
		for _, key := range snapshot.Io.Keys {
			if value, ok := snapshot.Io.Get(key); ok {
				fmt.Fprintf(w, " %s=%s", key, formatValue(value))
			}
		}
		fmt.Fprintln(w)
	}
}

// getOrderedMapField gets a field from an OrderedMap with type safety.
func getOrderedMapField(m client.OrderedMap, key string) interface{} {
	v, _ := m.Get(key)
	return v
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
		// Use JSON/object key order from the first row
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
