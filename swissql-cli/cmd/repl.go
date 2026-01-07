package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

func isMetaCommandStart(s string) bool {
	if s == "" {
		return false
	}
	return strings.HasPrefix(s, "\\") || strings.HasPrefix(strings.ToLower(s), "desc") || strings.HasPrefix(strings.ToLower(s), "explain") || strings.HasPrefix(s, "@") || strings.EqualFold(s, "conninfo")
}

func parseMetaCommand(input string) (string, []string) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", nil
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return "", nil
	}
	cmd := fields[0]
	args := []string{}
	if len(fields) > 1 {
		args = fields[1:]
	}
	return cmd, args
}

func readFileContent(baseDir string, path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("file path is required")
	}
	p := path
	if baseDir != "" && !filepath.IsAbs(path) {
		p = filepath.Join(baseDir, path)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func trimTrailingSemicolon(s string) string {
	t := strings.TrimSpace(s)
	if strings.HasSuffix(t, ";") {
		return strings.TrimSpace(strings.TrimSuffix(t, ";"))
	}
	return t
}

func shouldRecordHistory(mode string, input string, isSql bool) bool {
	if isSql {
		return true
	}

	s := strings.TrimSpace(input)
	if s == "" {
		return false
	}

	sLower := strings.ToLower(s)

	switch mode {
	case "all":
		return true
	case "sql_only":
		return false
	case "safe_only":
		if strings.HasPrefix(sLower, "/ai") || strings.HasPrefix(sLower, "/context") {
			return false
		}
		if strings.HasPrefix(s, "@") || strings.HasPrefix(s, "\\i") {
			return false
		}
		if strings.HasPrefix(s, "\\d") || strings.HasPrefix(sLower, "desc") {
			return true
		}
		if strings.HasPrefix(s, "\\dt") || strings.HasPrefix(s, "\\dv") {
			return true
		}
		if strings.HasPrefix(s, "\\explain") || strings.HasPrefix(sLower, "explain") {
			return true
		}
		if sLower == "conninfo" || sLower == "\\conninfo" {
			return true
		}
		if strings.HasPrefix(sLower, "set display ") {
			return true
		}
		return false
	default:
		return false
	}
}

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
		fmt.Println("Type 'help' for commands. Use 'detach' to leave without disconnecting.")
		fmt.Println("Type 'exit' or 'quit' to disconnect and remove this session.")
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

			lower := strings.ToLower(input)
			if lower == "help" {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				fmt.Println("Commands:")
				fmt.Println("  help                          Show this help")
				fmt.Println("  detach                        Leave REPL without disconnecting (like tmux detach)")
				fmt.Println("  exit | quit                   Disconnect backend session and remove it from registry")
				fmt.Println("  \\conninfo                    Show current session and backend information")
				fmt.Println("  \\d <name> (alias: desc)       Describe a table/view")
				fmt.Println("  \\d+ <name> (alias: desc+)     Describe with more details")
				fmt.Println("  \\dt | \\dv                     List tables/views")
				fmt.Println("  \\explain <sql> (alias: explain, explain plan for)")
				fmt.Println("                               Show execution plan")
				fmt.Println("  \\explain analyze <sql> (alias: explain analyze)")
				fmt.Println("                               Show actual execution plan (executes the statement)")
				fmt.Println("  \\i <file> (alias: @<file>)    Execute statements from a file")
				fmt.Println("  set display wide|narrow       Toggle truncation mode for tabular output")
				fmt.Println("  set display width <n>         Set max column width for tabular output")
				fmt.Println("  /ai <prompt>                  Generate SQL via AI and confirm before execution")
				fmt.Println("  /context show                 Show recent executed SQL context used by AI")
				fmt.Println("  /context clear                Clear AI context")
				fmt.Println("Notes:")
				fmt.Println("  - End a statement with ';' to execute")
				continue
			}
			if lower == "detach" {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				break
			}
			if lower == "exit" || lower == "quit" {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				if err := c.Disconnect(entry.SessionId); err != nil {
					fmt.Printf("%v\n", err)
				}

				resolvedName := name
				if strings.TrimSpace(resolvedName) == "" && cfg != nil {
					resolvedName = cfg.CurrentName
				}
				resolvedName = strings.TrimSpace(resolvedName)

				if resolvedName != "" {
					reg, err := config.LoadRegistry()
					if err == nil && reg != nil {
						reg.RemoveSession(resolvedName)
						_ = config.SaveRegistry(reg)
					}
					if cfg != nil && cfg.CurrentName == resolvedName {
						cfg.CurrentName = ""
						_ = config.SaveConfig(cfg)
					}
				}
				break
			}
			if strings.HasPrefix(lower, "set display ") {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
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

			// Phase 3 P0 meta-commands (single-line)
			if isMetaCommandStart(input) {
				cmdName, args := parseMetaCommand(input)
				cmdLower := strings.ToLower(cmdName)

				switch {
				case cmdLower == "\\conninfo" || cmdLower == "conninfo":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					fmt.Printf("Session name: %s\n", name)
					fmt.Printf("Session id:   %s\n", entry.SessionId)
					fmt.Printf("DB type:      %s\n", entry.DbType)
					fmt.Printf("Remote host:  %s\n", entry.GetRemoteHost())
					fmt.Printf("Backend:      %s\n", entry.ServerURL)
					continue

				case cmdLower == "desc" || cmdLower == "desc+" || cmdLower == "\\d" || cmdLower == "\\d+":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					if len(args) < 1 {
						fmt.Println("Error: desc requires an object name")
						continue
					}
					objName := trimTrailingSemicolon(args[0])
					if objName == "" {
						fmt.Println("Error: desc requires an object name")
						continue
					}
					detail := "basic"
					if cmdLower == "desc+" || cmdLower == "\\d+" {
						detail = "full"
					}
					resp, err := c.MetaDescribe(entry.SessionId, objName, detail)
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
					}
					renderResponse(cmd, resp)
					continue

				case cmdLower == "\\dt" || cmdLower == "\\dv":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					kind := "table"
					if cmdLower == "\\dv" {
						kind = "view"
					}
					resp, err := c.MetaList(entry.SessionId, kind, "")
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
					}
					renderResponse(cmd, resp)
					continue

				case cmdLower == "\\i" || strings.HasPrefix(cmdName, "@"):
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					fileArg := ""
					if cmdLower == "\\i" {
						if len(args) < 1 {
							fmt.Println("Error: \\i requires a file path")
							continue
						}
						fileArg = args[0]
					} else {
						fileArg = strings.TrimPrefix(cmdName, "@")
						fileArg = strings.TrimSpace(fileArg)
						if fileArg == "" {
							fmt.Println("Error: @ requires a file path")
							continue
						}
					}
					fileArg = trimTrailingSemicolon(fileArg)
					if fileArg == "" {
						fmt.Println("Error: file path is required")
						continue
					}

					content, err := readFileContent("", fileArg)
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
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
					if len(buf) > 0 {
						fmt.Println("Warning: trailing statement missing ';' was ignored")
					}
					continue

				case cmdLower == "\\explain" || cmdLower == "explain":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
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
						continue
					}
					resp, err := c.MetaExplain(entry.SessionId, sql, analyze)
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
					}
					renderResponse(cmd, resp)
					continue
				}
			}

			if lower == "/context show" {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
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
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
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

				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}

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
