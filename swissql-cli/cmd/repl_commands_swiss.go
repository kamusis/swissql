package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

type swissUsageLine struct {
	Command     string
	Description string
}

var swissUsage = []swissUsageLine{
	{Command: "\\swiss list", Description: "List available collectors"},
	{Command: "\\swiss list queries [--collector=<collector_id>]", Description: "List runnable query definitions under queries: in YAML packs"},
	{Command: "\\swiss run <query_id>", Description: "Run a query (auto-resolve)"},
	{Command: "\\swiss run <collector_id|collector_ref> <query_id>", Description: "Run a query (explicit collector)"},
}

func printSwissUsage(w io.Writer) {
	_ = w
	fmt.Println("Usage:")
	for _, l := range swissUsage {
		fmt.Printf("  %s\n", l.Command)
	}
}

// handleReplSwissCommands handles collector-related commands such as:
// \swiss list
// \swiss run <query_id>
// \swiss run <collector_id|collector_ref> <query_id>
func handleReplSwissCommands(
	_ *cobra.Command,
	line *liner.State,
	historyMode string,
	input string,
	cmdName string,
	args []string,
	c *client.Client,
	sessionId string,
	_ *config.Config,
) bool {
	cmdLower := strings.ToLower(cmdName)
	if cmdLower != "\\swiss" {
		return false
	}

	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	if len(args) < 1 {
		printSwissUsage(getOutputWriter())
		return true
	}

	action := strings.ToLower(strings.TrimSpace(args[0]))
	switch action {
	case "list":
		if len(args) >= 2 && strings.EqualFold(strings.TrimSpace(args[1]), "queries") {
			collectorId := ""
			for i := 2; i < len(args); i++ {
				a := strings.TrimSpace(args[i])
				if strings.HasPrefix(a, "--collector=") {
					collectorId = strings.TrimSpace(strings.TrimPrefix(a, "--collector="))
				}
			}

			resp, err := c.CollectorsQueriesList(sessionId, collectorId)
			if err != nil {
				fmt.Printf("%v\n", err)
				return true
			}
			if resp == nil || len(resp.Queries) == 0 {
				if strings.TrimSpace(collectorId) != "" {
					fmt.Printf("(no queries under queries: for collector_id=%s)\n", collectorId)
					return true
				}
				fmt.Println("(no queries under queries:)")
				return true
			}

			out := &client.ExecuteResponse{
				Type:   "tabular",
				Schema: "",
				Data: client.DataContent{
					Columns: []client.ColumnDefinition{
						{Name: "collector_ref", Type: "string"},
						{Name: "collector_id", Type: "string"},
						{Name: "query_id", Type: "string"},
						{Name: "description", Type: "string"},
					},
					Rows: []map[string]any{},
				},
				Metadata: client.ResponseMetadata{DurationMs: 0, RowsAffected: len(resp.Queries), Truncated: false},
			}
			for _, item := range resp.Queries {
				out.Data.Rows = append(out.Data.Rows, map[string]any{
					"collector_ref": item.CollectorRef,
					"collector_id":  item.CollectorId,
					"query_id":      item.QueryId,
					"description":   strings.TrimSpace(item.Description),
				})
			}
			renderResponse(nil, out)
			return true
		}

		resp, err := c.CollectorsList(sessionId)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		if resp == nil || len(resp.Collectors) == 0 {
			fmt.Println("(no collectors)")
			return true
		}

		out := &client.ExecuteResponse{
			Type:   "tabular",
			Schema: "",
			Data: client.DataContent{
				Columns: []client.ColumnDefinition{
					{Name: "collector_ref", Type: "string"},
					{Name: "collector_id", Type: "string"},
					{Name: "description", Type: "string"},
				},
				Rows: []map[string]any{},
			},
			Metadata: client.ResponseMetadata{DurationMs: 0, RowsAffected: len(resp.Collectors), Truncated: false},
		}
		for _, item := range resp.Collectors {
			out.Data.Rows = append(out.Data.Rows, map[string]any{
				"collector_ref": item.CollectorRef,
				"collector_id":  item.CollectorId,
				"description":   strings.TrimSpace(item.Description),
			})
		}
		renderResponse(nil, out)
		return true
	case "run":
		if len(args) < 2 {
			printSwissUsage(getOutputWriter())
			return true
		}

		// Shorthand: \swiss run <query_id>
		// Explicit:  \swiss run <collector_id|collector_ref> <query_id>
		queryId := ""
		req := &client.CollectorsRunRequest{SessionId: sessionId}

		// Collect any --key=value params.
		params := map[string]any{}
		extra := []string{}
		for i := 1; i < len(args); i++ {
			a := strings.TrimSpace(args[i])
			if strings.HasPrefix(a, "--") {
				kv := strings.TrimPrefix(a, "--")
				parts := strings.SplitN(kv, "=", 2)
				if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" {
					v := strings.TrimSpace(parts[1])
					if len(v) >= 2 {
						if (strings.HasPrefix(v, "\"") && strings.HasSuffix(v, "\"")) || (strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'")) {
							v = v[1 : len(v)-1]
						}
					}
					params[strings.TrimSpace(parts[0])] = v
					continue
				}
			}
			extra = append(extra, a)
		}

		// Shorthand: \swiss run <query_id> [--k=v ...]
		// Explicit:  \swiss run <collector_id|collector_ref> <query_id> [--k=v ...]
		if len(extra) == 1 {
			queryId = extra[0]
			req.QueryId = queryId
		} else if len(extra) >= 2 {
			// Explicit when collector_ref is provided (contains ':') or when collector_id is given with at least 3 tokens.
			if strings.Contains(extra[0], ":") {
				req.CollectorRef = extra[0]
				queryId = extra[1]
				req.QueryId = queryId
				if len(extra) > 2 {
					req.Args = extra[2:]
				}
			} else if len(extra) >= 3 {
				req.CollectorId = extra[0]
				queryId = extra[1]
				req.QueryId = queryId
				if len(extra) > 2 {
					req.Args = extra[2:]
				}
			} else {
				// Shorthand: \swiss run <query_id> <arg...>
				queryId = extra[0]
				req.QueryId = queryId
				req.Args = extra[1:]
			}
		}
		if len(params) > 0 {
			req.Params = params
		}
		if strings.TrimSpace(queryId) == "" {
			fmt.Println("Error: query_id is required")
			return true
		}

		_, qr, err := c.CollectorsRun(req)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		if qr == nil {
			fmt.Println("Error: empty response")
			return true
		}
		renderResponse(nil, &qr.Result)
		return true
	default:
		printSwissUsage(getOutputWriter())
		return true
	}
}
