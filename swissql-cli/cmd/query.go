package cmd

import (
	"fmt"
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
	if resp.Type == "tabular" {
		table := tablewriter.NewWriter(os.Stdout)
		// Preserve column names exactly as returned by the backend (e.g. TABLE_NAME).
		table.Options(tablewriter.WithConfig(tablewriter.Config{
			Header: tw.CellConfig{
				Formatting: tw.CellFormatting{AutoFormat: tw.Off},
			},
		}))

		// Check for --plain flag
		plain, _ := cmd.Flags().GetBool("plain")
		if plain {
			// Use ASCII symbols for perfect alignment in all terminals
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
				isPlanTableOutput := strings.EqualFold(col.Name, "PLAN_TABLE_OUTPUT")
				isQueryPlan := strings.EqualFold(col.Name, "QUERY PLAN") || strings.EqualFold(col.Name, "QUERY_PLAN")
				if isPlanTableOutput || isQueryPlan {
					cell = strings.ReplaceAll(cell, "\r\n", "\n")
					cell = strings.ReplaceAll(cell, "\t", " ")
				} else {
					cell = strings.ReplaceAll(cell, "\r\n", " ")
					cell = strings.ReplaceAll(cell, "\n", " ")
					cell = strings.ReplaceAll(cell, "\t", " ")
					if !displayWide {
						maxWidth := displayMaxColWidth
						if col.Name == "query" || col.Name == "QUERY" {
							maxWidth = displayMaxQueryWidth
						}
						cell = truncateWithEllipsisCell(cell, maxWidth)
					}
				}
				values[i] = cell
			}
			table.Append(values...)
		}
		table.Render()
		fmt.Printf("\n(%d rows, %d ms)\n", resp.Metadata.RowsAffected, resp.Metadata.DurationMs)
		if resp.Metadata.Truncated {
			fmt.Println("Warning: Results truncated to limit.")
		}
	} else {
		fmt.Println(resp.Data.TextContent)
		fmt.Printf("\n(%d ms)\n", resp.Metadata.DurationMs)
	}
}

func init() {
	cfg, err := config.LoadConfig()
	if err == nil && cfg != nil {
		displayWide = cfg.DisplayWide
		displayMaxColWidth = cfg.Display.MaxColWidth
		displayMaxQueryWidth = cfg.Display.MaxQueryWidth
	}
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().String("name", "", "Session name to use (tmux-like)")
}
