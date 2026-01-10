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
	_ = args

	name := ""
	if cmd.Flags().Lookup("name") != nil {
		name, _ = cmd.Flags().GetString("name")
	}

	server := ""
	if cmd.Flags().Lookup("server") != nil {
		server, _ = cmd.Flags().GetString("server")
	} else if cmd.InheritedFlags().Lookup("server") != nil {
		server, _ = cmd.InheritedFlags().GetString("server")
	}

	entry, err := config.ResolveActiveSession(name)
	if err != nil {
		// Empty REPL: backend-only session, no DB connection yet.
		if strings.TrimSpace(server) == "" {
			server = "http://localhost:8080"
		}
		entry = config.SessionEntry{ServerURL: server}
		name = ""
	} else {
		// Prefer the persisted server URL for attached sessions.
		if strings.TrimSpace(entry.ServerURL) != "" {
			server = entry.ServerURL
		}
		if strings.TrimSpace(server) == "" {
			server = "http://localhost:8080"
		}

		// If a session name is explicitly provided, persist it as current.
		if name != "" {
			cfg, err := config.LoadConfig()
			if err != nil {
				return err
			}
			cfg.CurrentName = name
			if err := config.SaveConfig(cfg); err != nil {
				fmt.Printf("Warning: could not save config: %v\n", err)
			}
			_ = config.TouchSession(name)
		}
	}

	timeout, _ := cmd.Flags().GetInt("connection-timeout")
	c := client.NewClient(server, time.Duration(timeout)*time.Millisecond)

	sessionId := entry.SessionId
	currentDbType := ""
	if strings.TrimSpace(sessionId) != "" {
		currentDbType = entry.DbType
	}

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
	line.SetCompleter(makeCompleter(c, sessionId))

	historyPath, _ := config.GetHistoryPath()
	if f, err := os.Open(historyPath); err == nil {
		line.ReadHistory(f)
		f.Close()
	}

	if strings.TrimSpace(sessionId) == "" {
		fmt.Printf("SwissQL REPL (Backend: %s)\n", server)
		fmt.Println("Type 'help' for commands. Use 'detach' to leave.")
		fmt.Println("Use 'connect <dsn>' to connect to a database.")
	} else {
		fmt.Printf("SwissQL REPL (Session: %s)\n", sessionId)
		fmt.Println("Type 'help' for commands. Use 'detach' to leave without disconnecting.")
		fmt.Println("Type 'exit' or 'quit' to disconnect and remove this session.")
		fmt.Println("Use '/ai <prompt>' to generate SQL via backend and confirm before execution.")
	}

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
			sessionId,
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

		if handleReplSetDbType(cmd, line, cfg.History.Mode, input, c, sessionId, &currentDbType) {
			continue
		}

		if handleReplDriverCommands(cmd, line, cfg.History.Mode, input, c) {
			continue
		}

		connected, newEntry, newName := handleReplConnectCommand(cmd, line, cfg.History.Mode, input, c)
		if connected {
			sessionId = newEntry.SessionId
			entry = newEntry
			currentDbType = entry.DbType
			name = newName
			line.SetCompleter(makeCompleter(c, sessionId))
			invalidateCache()
			fmt.Printf("Connected successfully! Session ID: %s\n", sessionId)
			continue
		}

		// Phase 3 P0 meta-commands (single-line)
		if isMetaCommandStart(input) {
			cmdName, args := parseMetaCommand(input)

			// Check if it's a watch command (can run without session)
			if handleReplWatch(cmd, line, cfg.History.Mode, input, cmdName, args, c, sessionId, cfg) {
				continue
			}

			if strings.TrimSpace(sessionId) == "" {
				fmt.Println("Error: no active DB session. Use 'connect <dsn>' first.")
				continue
			}

			if handleReplMetaCommands(cmd, line, cfg.History.Mode, input, cmdName, args, c, sessionId, cfg) {
				continue
			}
			if handleReplSamplerCommands(cmd, line, cfg.History.Mode, input, cmdName, args, c, sessionId, cfg) {
				continue
			}
			if handleReplTopCommands(cmd, line, cfg.History.Mode, input, cmdName, args, c, sessionId, cfg) {
				continue
			}
			if handleReplIOCommands(cmd, line, cfg.History.Mode, input, cmdName, args, c, sessionId) {
				continue
			}
		}

		if strings.TrimSpace(sessionId) != "" {
			if handleReplContextCommands(line, cfg.History.Mode, input, lower, c, sessionId) {
				continue
			}
		} else if strings.HasPrefix(lower, "/context") {
			fmt.Println("Error: no active DB session. Use 'connect <dsn>' first.")
			continue
		}

		if handleReplAICommand(cmd, line, cfg.History.Mode, input, c, sessionId, currentDbType, &multiLineSql) {
			continue
		}

		multiLineSql = append(multiLineSql, input)

		if strings.HasSuffix(input, ";") {
			if strings.TrimSpace(sessionId) == "" {
				fmt.Println("Error: no active DB session. Use 'connect <dsn>' first.")
				multiLineSql = nil
				continue
			}
			sql := strings.Join(multiLineSql, "\n")
			sql = strings.TrimSuffix(sql, ";")
			multiLineSql = nil

			line.AppendHistory(strings.ReplaceAll(sql, "\n", " ") + ";")

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

	if f, err := os.Create(historyPath); err != nil {
		fmt.Printf("Warning: could not create history file: %v\n", err)
	} else {
		defer func() {
			if err := f.Close(); err != nil {
				fmt.Printf("Warning: could not close history file: %v\n", err)
			}
		}()
		if _, err := line.WriteHistory(f); err != nil {
			fmt.Printf("Warning: could not write history: %v\n", err)
		}
	}

	return nil
}
