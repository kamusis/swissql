package cmd

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Start an interactive SQL REPL",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		entry, err := config.ResolveActiveSession(name)
		if err != nil {
			return err
		}

		if name != "" {
			cfg, err := config.LoadConfig()
			if err != nil {
				return err
			}
			cfg.CurrentName = name
			_ = config.SaveConfig(cfg)
			_ = config.TouchSession(name)
		}

		server := entry.ServerURL
		timeout, _ := cmd.Flags().GetInt("connection-timeout")
		c := client.NewClient(server, time.Duration(timeout)*time.Millisecond)

		line := liner.NewLiner()
		defer line.Close()

		line.SetCtrlCAborts(true)

		historyPath, _ := config.GetHistoryPath()
		if f, err := os.Open(historyPath); err == nil {
			line.ReadHistory(f)
			f.Close()
		}

		fmt.Printf("SwissQL REPL (Session: %s)\n", entry.SessionId)
		fmt.Println("Type 'exit' or 'quit' to leave, ';' at the end of a line to execute.")
		fmt.Println("Use '/ai <prompt>' to generate SQL via backend and confirm before execution.")

		var multiLineSql []string

		for {
			prompt := "swissql> "
			if len(multiLineSql) > 0 {
				prompt = "      -> "
			}

			input, err := line.Prompt(prompt)
			if err != nil {
				if err == liner.ErrPromptAborted {
					fmt.Println("\nAborting...")
					// Detach by default (do not disconnect)
					break
				}
				return err
			}

			input = strings.TrimSpace(input)
			if input == "" {
				continue
			}

			if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
				// Detach by default (do not disconnect)
				break
			}

			if strings.HasPrefix(input, "/ai ") {
				multiLineSql = nil
				promptText := strings.TrimSpace(strings.TrimPrefix(input, "/ai "))
				if promptText == "" {
					fmt.Println("Error: /ai requires a prompt")
					continue
				}

				line.AppendHistory(input)

				aResp, err := c.AiGenerate(&client.AiGenerateRequest{
					Prompt: promptText,
					DbType: entry.DbType,
				})
				if err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				}

				if len(aResp.Warnings) > 0 {
					for _, w := range aResp.Warnings {
						fmt.Printf("Warning: %s\n", w)
					}
				}

				if strings.TrimSpace(aResp.Sql) == "" {
					fmt.Println("No SQL generated.")
					continue
				}

				fmt.Println("Generated SQL:")
				fmt.Println(aResp.Sql)

				yes, _ := cmd.Flags().GetBool("yes")
				execute := yes
				if !yes {
					confirm, err := line.Prompt("Execute? [Y/n] ")
					if err != nil {
						fmt.Printf("Error: %v\n", err)
						continue
					}
					confirm = strings.TrimSpace(confirm)
					execute = confirm == "" || strings.EqualFold(confirm, "y") || strings.EqualFold(confirm, "yes")
					if strings.EqualFold(confirm, "n") || strings.EqualFold(confirm, "no") {
						execute = false
					}
				}

				if !execute {
					fmt.Println("Aborted.")
					continue
				}

				req := &client.ExecuteRequest{
					SessionId: entry.SessionId,
					Sql:       aResp.Sql,
					Options: client.ExecuteOptions{
						Limit:          0,
						FetchSize:      50,
						QueryTimeoutMs: 0,
					},
				}

				resp, err := c.Execute(req)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
					continue
				}
				renderResponse(cmd, resp)
				continue
			}

			multiLineSql = append(multiLineSql, input)

			if strings.HasSuffix(input, ";") {
				sql := strings.Join(multiLineSql, "\n")
				sql = strings.TrimSuffix(sql, ";")
				multiLineSql = nil

				line.AppendHistory(sql + ";")

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
					fmt.Printf("Error: %v\n", err)
					continue
				}

				renderResponse(cmd, resp)
			}
		}

		if f, err := os.Create(historyPath); err == nil {
			line.WriteHistory(f)
			f.Close()
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(replCmd)
	replCmd.Flags().String("name", "", "Session name to use (tmux-like)")
}
