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

func runRepl(cmd *cobra.Command, args []string) error {
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
		setDisplayExpanded(cfg.DisplayExpanded)
		setDisplayWidth(cfg.Display.MaxColWidth)
		setDisplayQueryWidth(cfg.Display.MaxQueryWidth)
		_ = setOutputFormat(cfg.OutputFormat)
	}

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)
	line.SetCompleter(makeCompleter(c, entry.SessionId))

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
				fmt.Println("^C")
				multiLineSql = nil
				continue
			}
			return err
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		lower := strings.ToLower(input)

		if handleReplHelp(line, cfg.History.Mode, input) {
			continue
		}

		handled, shouldBreak := handleReplDetachExit(
			cmd,
			line,
			cfg.History.Mode,
			input,
			c,
			entry.SessionId,
			name,
			cfg,
		)
		if handled {
			if shouldBreak {
				break
			}
			continue
		}

		if handleReplSetDisplay(line, cfg.History.Mode, input, cfg) {
			continue
		}

		if handleReplSetOutput(line, cfg.History.Mode, input, cfg) {
			continue
		}

		// Phase 3 P0 meta-commands (single-line)
		if isMetaCommandStart(input) {
			cmdName, args := parseMetaCommand(input)

			if handleReplMetaCommands(cmd, line, cfg.History.Mode, input, cmdName, args, c, entry.SessionId, cfg) {
				continue
			}

			if handleReplIOCommands(cmd, line, cfg.History.Mode, input, cmdName, args, c, entry.SessionId) {
				continue
			}
		}

		if handleReplContextCommands(line, cfg.History.Mode, input, lower, c, entry.SessionId) {
			continue
		}

		if handleReplAICommand(cmd, line, cfg.History.Mode, input, c, entry.SessionId, entry.DbType, &multiLineSql) {
			continue
		}

		multiLineSql = append(multiLineSql, input)

		if strings.HasSuffix(input, ";") {
			sql := strings.Join(multiLineSql, "\n")
			sql = strings.TrimSuffix(sql, ";")
			multiLineSql = nil

			line.AppendHistory(strings.ReplaceAll(sql, "\n", " ") + ";")

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
}
