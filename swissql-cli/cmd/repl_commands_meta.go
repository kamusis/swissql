package cmd

import (
	"fmt"
	"strings"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

// handleReplMetaCommands handles meta commands related to session metadata / introspection
// such as describing objects (\\d/desc), listing tables/views (\\dt/\\dv), showing connection
// info (\\conninfo), toggling expanded display (\\x), and explain (\\explain).
func handleReplMetaCommands(
	cmd *cobra.Command,
	line *liner.State,
	historyMode string,
	input string,
	cmdName string,
	args []string,
	c *client.Client,
	sessionId string,
	cfg *config.Config,
) bool {
	cmdLower := strings.ToLower(cmdName)

	switch {
	case cmdLower == "\\x":
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		if len(args) == 0 {
			setDisplayExpanded(!displayExpanded)
			if cfg != nil {
				cfg.DisplayExpanded = displayExpanded
				if err := config.SaveConfig(cfg); err != nil {
					fmt.Printf("Warning: could not save config: %v\n", err)
				}
			}
			if displayExpanded {
				fmt.Println("Expanded display mode enabled.")
			} else {
				fmt.Println("Expanded display mode disabled.")
			}
			return true
		}
		switch strings.ToLower(strings.TrimSpace(args[0])) {
		case "on":
			setDisplayExpanded(true)
			if cfg != nil {
				cfg.DisplayExpanded = true
				if err := config.SaveConfig(cfg); err != nil {
					fmt.Printf("Warning: could not save config: %v\n", err)
				}
			}
			fmt.Println("Expanded display mode enabled.")
			return true
		case "off":
			setDisplayExpanded(false)
			if cfg != nil {
				cfg.DisplayExpanded = false
				if err := config.SaveConfig(cfg); err != nil {
					fmt.Printf("Warning: could not save config: %v\n", err)
				}
			}
			fmt.Println("Expanded display mode disabled.")
			return true
		default:
			fmt.Println("Usage: \\x [on|off]")
			return true
		}

	case cmdLower == "\\conninfo" || cmdLower == "conninfo":
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		resp, err := c.MetaConninfo(sessionId)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		renderResponse(cmd, resp)
		return true

	case cmdLower == "desc" || cmdLower == "desc+" || cmdLower == "\\d" || cmdLower == "\\d+":
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		if len(args) < 1 {
			fmt.Println("Error: desc requires an object name")
			return true
		}
		objName := trimTrailingSemicolon(args[0])
		if objName == "" {
			fmt.Println("Error: desc requires an object name")
			return true
		}
		detail := "basic"
		if cmdLower == "desc+" || cmdLower == "\\d+" {
			detail = "full"
		}
		resp, err := c.MetaDescribe(sessionId, objName, detail)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		renderResponse(cmd, resp)
		return true

	case cmdLower == "\\dt" || cmdLower == "\\dv":
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		kind := "table"
		if cmdLower == "\\dv" {
			kind = "view"
		}
		resp, err := c.MetaList(sessionId, kind, "")
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		renderResponse(cmd, resp)
		return true

	case cmdLower == "\\explain" || cmdLower == "explain":
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		analyze := false
		sql := ""
		if len(args) > 0 {
			sql = strings.Join(args, " ")
		}
		// Accept analyze form: "\\explain analyze <sql>" and alias "explain analyze <sql>"
		if strings.HasPrefix(strings.ToLower(sql), "analyze ") {
			analyze = true
			sql = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(sql), "analyze "))
		}
		// Accept alias form: "explain plan for <sql>"
		if strings.HasPrefix(strings.ToLower(sql), "plan for ") {
			sql = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(sql), "plan for "))
		}
		sql = trimTrailingSemicolon(sql)
		if strings.TrimSpace(sql) == "" {
			fmt.Println("Error: explain requires a SQL statement")
			return true
		}
		resp, err := c.MetaExplain(sessionId, sql, analyze)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		renderResponse(cmd, resp)
		return true

	case cmdLower == "\\sqltext":
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		if len(args) < 1 {
			fmt.Println("Usage: \\sqltext <sql_id>")
			return true
		}
		sqlId := args[0]
		sqlText, err := c.GetSqlText(sessionId, sqlId)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return true
		}
		resp := &client.ExecuteResponse{
			Type:   "tabular",
			Schema: "",
			Data: client.DataContent{
				Columns: []client.ColumnDefinition{
					{Name: "sql_id", Type: "text"},
					{Name: "text", Type: "text"},
					{Name: "truncated", Type: "bool"},
				},
				Rows: []map[string]interface{}{
					{
						"sql_id":    sqlText.SqlId,
						"text":      sqlText.Text,
						"truncated": sqlText.Truncated,
					},
				},
			},
			Metadata: client.ResponseMetadata{
				DurationMs:   0,
				RowsAffected: 1,
				Truncated:    false,
			},
		}
		renderResponse(cmd, resp)
		return true

	case cmdLower == "\\plan":
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		if len(args) < 1 {
			fmt.Println("Usage: \\plan <sql_id>")
			return true
		}
		sqlId := args[0]
		// Fetch SQL text first
		sqlText, err := c.GetSqlText(sessionId, sqlId)
		if err != nil {
			fmt.Printf("Error fetching SQL text: %v\n", err)
			return true
		}
		// Get execution plan using existing MetaExplain
		resp, err := c.MetaExplain(sessionId, sqlText.Text, false)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return true
		}
		renderResponse(cmd, resp)
		return true
	}

	return false
}
