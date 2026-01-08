package cmd

import (
	"fmt"
	"strings"

	"github.com/kamusis/swissql/swissql-cli/internal/client"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

// handleReplIOCommands handles IO-related meta commands such as output redirection (\\o)
// and executing SQL from a file (\\i and @file).
func handleReplIOCommands(
	cmd *cobra.Command,
	line *liner.State,
	historyMode string,
	input string,
	cmdName string,
	args []string,
	c *client.Client,
	sessionId string,
) bool {
	cmdLower := strings.ToLower(cmdName)

	switch {
	case cmdLower == "\\o":
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		if len(args) == 0 {
			if err := resetOutputWriter(); err != nil {
				fmt.Printf("Error: %v\n", err)
				return true
			}
			fmt.Println("Output restored to stdout.")
			return true
		}
		path := trimTrailingSemicolon(args[0])
		if err := setOutputFile(path); err != nil {
			fmt.Printf("Error: %v\n", err)
			return true
		}
		fmt.Printf("Output redirected to %s.\n", path)
		return true

	case cmdLower == "\\i" || strings.HasPrefix(cmdName, "@"):
		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory(input)
		}
		fileArg := ""
		if cmdLower == "\\i" {
			if len(args) < 1 {
				fmt.Println("Error: \\i requires a file path")
				return true
			}
			fileArg = args[0]
		} else {
			fileArg = strings.TrimPrefix(cmdName, "@")
			fileArg = strings.TrimSpace(fileArg)
			if fileArg == "" {
				fmt.Println("Error: @ requires a file path")
				return true
			}
		}
		fileArg = trimTrailingSemicolon(fileArg)
		if fileArg == "" {
			fmt.Println("Error: file path is required")
			return true
		}

		content, err := readFileContent("", fileArg)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		lines := strings.Split(content, "\n")
		buf := make([]string, 0)
		for _, l := range lines {
			lineText := strings.TrimSpace(l)
			if lineText == "" {
				continue
			}
			buf = append(buf, lineText)
			if strings.HasSuffix(lineText, ";") {
				sql := strings.Join(buf, "\n")
				sql = strings.TrimSuffix(sql, ";")
				buf = nil

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
					fmt.Printf("%v\n", err)
					continue
				}
				renderResponse(cmd, resp)
			}
		}
		if len(buf) > 0 {
			fmt.Println("Warning: trailing statement missing ';' was ignored")
		}
		return true
	}

	return false
}
