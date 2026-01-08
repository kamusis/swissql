package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kamusis/swissql/swissql-cli/internal/client"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

// handleReplContextCommands handles the /context commands.
func handleReplContextCommands(
	line *liner.State,
	historyMode string,
	input string,
	lower string,
	c *client.Client,
	sessionId string,
) bool {
	if lower == "/context show" {
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		ctxResp, err := c.AiContext(sessionId, 10)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		if ctxResp == nil || len(ctxResp.Items) == 0 {
			fmt.Println("No AI context recorded for this session.")
			return true
		}
		for i, item := range ctxResp.Items {
			fmt.Printf("[%d] SQL: %s\n", i+1, item.Sql)
			if strings.TrimSpace(item.Error) != "" {
				fmt.Printf("    ERROR: %s\n", item.Error)
			}
			if len(item.Columns) > 0 {
				fmt.Println("    Columns:")
				for _, col := range item.Columns {
					fmt.Printf("      - %s %s\n", col.Name, col.Type)
				}
			}
			if len(item.SampleRows) > 0 {
				fmt.Println("    Sample rows:")
				for _, row := range item.SampleRows {
					fmt.Printf("      - %v\n", row)
				}
			}
		}
		return true
	}

	if lower == "/context clear" {
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		if err := c.AiContextClear(sessionId); err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		fmt.Println("AI context cleared.")
		return true
	}

	return false
}

// handleReplAICommand handles /ai prompt generation and optional execution.
func handleReplAICommand(
	cmd *cobra.Command,
	line *liner.State,
	historyMode string,
	input string,
	c *client.Client,
	sessionId string,
	dbType string,
	multiLineSql *[]string,
) bool {
	if !strings.HasPrefix(input, "/ai ") {
		return false
	}

	*multiLineSql = nil
	promptText := strings.TrimSpace(strings.TrimPrefix(input, "/ai "))
	if promptText == "" {
		fmt.Println("Error: /ai requires a prompt")
		return true
	}

	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	aResp, err := c.AiGenerate(&client.AiGenerateRequest{
		Prompt:       promptText,
		DbType:       dbType,
		SessionId:    sessionId,
		ContextMode:  "schema_and_samples",
		ContextLimit: 10,
	})
	if err != nil {
		fmt.Printf("%v\n", err)
		return true
	}

	if len(aResp.Warnings) > 0 {
		for _, w := range aResp.Warnings {
			fmt.Printf("Warning: %s\n", w)
		}
	}

	if strings.TrimSpace(aResp.Sql) == "" {
		fmt.Println("No SQL generated.")
		return true
	}

	type aiStatementsPayload struct {
		Statements []string `json:"statements"`
	}
	var payload aiStatementsPayload
	if err := json.Unmarshal([]byte(aResp.Sql), &payload); err != nil {
		fmt.Println("Error: AI output is not valid JSON.")
		return true
	}
	if len(payload.Statements) == 0 {
		fmt.Println("No SQL statements generated.")
		return true
	}

	statements := make([]string, 0, len(payload.Statements))
	for _, s := range payload.Statements {
		sql := trimTrailingSemicolon(s)
		if strings.TrimSpace(sql) == "" {
			continue
		}
		statements = append(statements, sql)
	}
	if len(statements) == 0 {
		fmt.Println("No SQL statements generated.")
		return true
	}

	fmt.Println("Generated SQL:")
	for i, sql := range statements {
		displaySql := strings.TrimSpace(sql)
		if displaySql != "" && !strings.HasSuffix(displaySql, ";") {
			displaySql += ";"
		}
		fmt.Printf("[%d] %s\n", i+1, displaySql)
	}

	yes, _ := cmd.Flags().GetBool("yes")
	execute := yes
	if !yes {
		confirm, err := line.Prompt("Execute? [Y/n] ")
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		confirm = strings.TrimSpace(confirm)
		execute = confirm == "" || strings.EqualFold(confirm, "y") || strings.EqualFold(confirm, "yes")
		if strings.EqualFold(confirm, "n") || strings.EqualFold(confirm, "no") {
			execute = false
		}
	}

	if !execute {
		fmt.Println("Aborted.")
		return true
	}

	for i, sql := range statements {
		req := &client.ExecuteRequest{
			SessionId: sessionId,
			Sql:       sql,
			Options: client.ExecuteOptions{
				Limit:          0,
				FetchSize:      50,
				QueryTimeoutMs: 0,
			},
		}

		resp, err := c.Execute(req)
		if err != nil {
			fmt.Printf("Statement [%d] failed: %v\n", i+1, err)
			break
		}
		renderResponse(cmd, resp)
	}

	return true
}
