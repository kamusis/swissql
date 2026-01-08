package cmd

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/spf13/cobra"
)

var displayWide bool
var displayMaxColWidth = 32
var displayMaxQueryWidth = 60

var displayExpanded bool

var outputFormat = "table"

var outputWriter io.Writer = os.Stdout
var outputFile *os.File

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func truncateWithEllipsisCell(s string, width int) string {
	if width <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= width {
		return s
	}
	if width <= 3 {
		return string(r[:width])
	}
	return string(r[:width-3]) + "..."
}

func setDisplayWide(v bool) {
	displayWide = v
}

func setDisplayWidth(width int) {
	displayMaxColWidth = clampInt(width, 8, 400)
}

func setDisplayQueryWidth(width int) {
	displayMaxQueryWidth = clampInt(width, 8, 2000)
}

func setDisplayExpanded(v bool) {
	displayExpanded = v
}

func isSupportedOutputFormat(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "table", "csv", "tsv", "json":
		return true
	default:
		return false
	}
}

func setOutputFormat(s string) error {
	f := strings.ToLower(strings.TrimSpace(s))
	if !isSupportedOutputFormat(f) {
		return fmt.Errorf("unsupported output format: %s", s)
	}
	outputFormat = f
	return nil
}

func setOutputFile(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("file path is required")
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if outputFile != nil {
		_ = outputFile.Close()
	}
	outputFile = f
	outputWriter = f
	return nil
}

func resetOutputWriter() error {
	if outputFile != nil {
		if err := outputFile.Close(); err != nil {
			return err
		}
		outputFile = nil
	}
	outputWriter = os.Stdout
	return nil
}

func parseDisplayWidthArg(s string) (int, error) {
	return strconv.Atoi(s)
}

var queryCmd = &cobra.Command{
	Use:   "query [SQL]",
	Short: "Execute a SQL query",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		sql := args[0]
		name, _ := cmd.Flags().GetString("name")
		entry, err := config.ResolveActiveSession(name)
		if err != nil {
			return err
		}

		server := entry.ServerURL
		timeout, _ := cmd.Flags().GetInt("connection-timeout")

		c := client.NewClient(server, time.Duration(timeout)*time.Millisecond)
		req := &client.ExecuteRequest{
			SessionId: entry.SessionId,
			Sql:       sql,
			Options: client.ExecuteOptions{
				Limit:          0,
				FetchSize:      50,
				QueryTimeoutMs: 0,
			},
		}

		resp, err := c.Execute(req)
		if err != nil {
			return err
		}

		renderResponse(cmd, resp)
		return nil
	},
}

func renderResponse(cmd *cobra.Command, resp *client.ExecuteResponse) {
	w := getOutputWriter()

	switch strings.ToLower(resp.Type) {
	case "tabular":
		switch outputFormat {
		case "json":
			renderTabularJSON(w, resp)
		case "csv":
			renderTabularDelimited(w, resp, ',')
		case "tsv":
			renderTabularDelimited(w, resp, '\t')
		default:
			if displayExpanded {
				renderTabularExpanded(w, resp)
				return
			}
			renderTabularTable(cmd, w, resp)
		}
	default:
		renderTextResponse(w, resp)
	}
}

func getOutputWriter() io.Writer {
	w := outputWriter
	if w == nil {
		w = os.Stdout
	}
	return w
}

func writeTabularFooter(w io.Writer, resp *client.ExecuteResponse) {
	fmt.Fprintf(w, "\n(%d rows, %d ms)\n", resp.Metadata.RowsAffected, resp.Metadata.DurationMs)
	if resp.Metadata.Truncated {
		fmt.Fprintln(w, "Warning: Results truncated to limit.")
	}
}

func renderTabularJSON(w io.Writer, resp *client.ExecuteResponse) {
	b, err := json.Marshal(resp.Data.Rows)
	if err != nil {
		fmt.Fprintf(w, "%v\n", err)
		return
	}
	fmt.Fprintf(w, "%s\n", string(b))
	writeTabularFooter(w, resp)
}

func renderTabularDelimited(w io.Writer, resp *client.ExecuteResponse, comma rune) {
	csvWriter := csv.NewWriter(w)
	csvWriter.Comma = comma

	headers := make([]string, len(resp.Data.Columns))
	for i, col := range resp.Data.Columns {
		headers[i] = col.Name
	}
	_ = csvWriter.Write(headers)

	for _, row := range resp.Data.Rows {
		values := make([]string, len(resp.Data.Columns))
		for i, col := range resp.Data.Columns {
			cell := fmt.Sprintf("%v", row[col.Name])
			values[i] = normalizeCellForDelimited(cell)
		}
		_ = csvWriter.Write(values)
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		fmt.Fprintf(w, "%v\n", err)
		return
	}
	writeTabularFooter(w, resp)
}

func normalizeCellForDelimited(cell string) string {
	return strings.ReplaceAll(cell, "\r\n", "\n")
}

func renderTabularExpanded(w io.Writer, resp *client.ExecuteResponse) {
	for rowIdx, row := range resp.Data.Rows {
		if rowIdx > 0 {
			fmt.Fprintln(w)
		}
		for _, col := range resp.Data.Columns {
			cell := fmt.Sprintf("%v", row[col.Name])
			cell = normalizeCellForExpanded(col.Name, cell)
			fmt.Fprintf(w, "%s: %s\n", col.Name, cell)
		}
	}
	writeTabularFooter(w, resp)
}

func normalizeCellForExpanded(colName string, cell string) string {
	cell = strings.ReplaceAll(cell, "\r\n", "\n")
	cell = strings.ReplaceAll(cell, "\t", " ")
	if displayWide {
		return cell
	}

	maxWidth := getMaxWidthForColumn(colName)
	cell = truncateWithEllipsisCell(strings.ReplaceAll(cell, "\n", " "), maxWidth)
	return cell
}

func renderTabularTable(cmd *cobra.Command, w io.Writer, resp *client.ExecuteResponse) {
	table := tablewriter.NewWriter(w)
	table.Options(tablewriter.WithConfig(tablewriter.Config{
		Header: tw.CellConfig{
			Formatting: tw.CellFormatting{AutoFormat: tw.Off},
		},
	}))

	plain, _ := cmd.Flags().GetBool("plain")
	if plain {
		table.Options(tablewriter.WithSymbols(&tw.SymbolASCII{}))
	}

	headers := make([]any, len(resp.Data.Columns))
	for i, col := range resp.Data.Columns {
		headers[i] = col.Name
	}
	table.Header(headers...)

	for _, row := range resp.Data.Rows {
		values := make([]any, len(resp.Data.Columns))
		for i, col := range resp.Data.Columns {
			cell := fmt.Sprintf("%v", row[col.Name])
			values[i] = normalizeCellForTable(col.Name, cell)
		}
		table.Append(values...)
	}

	table.Render()
	writeTabularFooter(w, resp)
}

func normalizeCellForTable(colName string, cell string) string {
	if isPlanLikeColumn(colName) {
		cell = strings.ReplaceAll(cell, "\r\n", "\n")
		cell = strings.ReplaceAll(cell, "\t", " ")
		return cell
	}

	cell = strings.ReplaceAll(cell, "\r\n", " ")
	cell = strings.ReplaceAll(cell, "\n", " ")
	cell = strings.ReplaceAll(cell, "\t", " ")
	if displayWide {
		return cell
	}

	maxWidth := getMaxWidthForColumn(colName)
	return truncateWithEllipsisCell(cell, maxWidth)
}

func isPlanLikeColumn(colName string) bool {
	return strings.EqualFold(colName, "PLAN_TABLE_OUTPUT") ||
		strings.EqualFold(colName, "QUERY PLAN") ||
		strings.EqualFold(colName, "QUERY_PLAN")
}

func getMaxWidthForColumn(colName string) int {
	if colName == "query" || colName == "QUERY" {
		return displayMaxQueryWidth
	}
	return displayMaxColWidth
}

func renderTextResponse(w io.Writer, resp *client.ExecuteResponse) {
	fmt.Fprintln(w, resp.Data.TextContent)
	fmt.Fprintf(w, "\n(%d ms)\n", resp.Metadata.DurationMs)
}

func init() {
	cfg, err := config.LoadConfig()
	if err == nil && cfg != nil {
		displayWide = cfg.DisplayWide
		displayExpanded = cfg.DisplayExpanded
		displayMaxColWidth = cfg.Display.MaxColWidth
		displayMaxQueryWidth = cfg.Display.MaxQueryWidth
		if err := setOutputFormat(cfg.OutputFormat); err == nil {
			// already set
		}
	}
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().String("name", "", "Session name to use (tmux-like)")
}
