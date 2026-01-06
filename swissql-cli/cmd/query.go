package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/spf13/cobra"
)

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
				values[i] = fmt.Sprintf("%v", row[col.Name])
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
	rootCmd.AddCommand(queryCmd)
	queryCmd.Flags().String("name", "", "Session name to use (tmux-like)")
}
