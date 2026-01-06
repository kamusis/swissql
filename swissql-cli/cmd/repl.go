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

		// Load persisted CLI display settings
		cfg, err := config.LoadConfig()
		if err == nil && cfg != nil {
			setDisplayWide(cfg.DisplayWide)
			setDisplayWidth(cfg.Display.MaxColWidth)
			setDisplayQueryWidth(cfg.Display.MaxQueryWidth)
		}

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

			lower := strings.ToLower(input)
			if strings.HasPrefix(lower, "set display ") {
				args := strings.Fields(lower)
				if len(args) == 3 {
					switch args[2] {
					case "wide":
						setDisplayWide(true)
						if cfg != nil {
							cfg.DisplayWide = true
							_ = config.SaveConfig(cfg)
						}
						fmt.Println("Display mode set to wide.")
						continue
					case "narrow":
						setDisplayWide(false)
						if cfg != nil {
							cfg.DisplayWide = false
							_ = config.SaveConfig(cfg)
						}
						fmt.Println("Display mode set to narrow.")
						continue
					}
				}
				if len(args) == 4 && args[2] == "width" {
					w, err := parseDisplayWidthArg(args[3])
					if err != nil {
						fmt.Println("Error: invalid width")
						continue
					}
					setDisplayWidth(w)
					if cfg != nil {
						cfg.Display.MaxColWidth = displayMaxColWidth
						_ = config.SaveConfig(cfg)
					}
					fmt.Printf("Display column width set to %d.\n", displayMaxColWidth)
					continue
				}
				fmt.Println("Usage: set display wide|narrow|width <n>")
				continue
			}

			if lower == "/context show" {
				line.AppendHistory(input)
				ctxResp, err := c.AiContext(entry.SessionId, 10)
				if err != nil {
					fmt.Printf("%v\n", err)
					continue
				}
				if ctxResp == nil || len(ctxResp.Items) == 0 {
					fmt.Println("No AI context recorded for this session.")
					continue
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
				continue
			}

			if lower == "/context clear" {
				line.AppendHistory(input)
				if err := c.AiContextClear(entry.SessionId); err != nil {
					fmt.Printf("%v\n", err)
					continue
				}
				fmt.Println("AI context cleared.")
				continue
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
					Prompt:       promptText,
					DbType:       entry.DbType,
					SessionId:    entry.SessionId,
					ContextMode:  "schema_and_samples",
					ContextLimit: 10,
				})
				if err != nil {
					fmt.Printf("%v\n", err)
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
						fmt.Printf("%v\n", err)
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
					fmt.Printf("%v\n", err)
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
					fmt.Printf("%v\n", err)
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
