package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

// handleReplHelp prints the REPL help text.
func handleReplHelp(line *liner.State, historyMode string, input string) bool {
	if !strings.EqualFold(strings.TrimSpace(input), "help") {
		return false
	}
	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	fmt.Println("Commands:")
	fmt.Println("")
	fmt.Println("[CLI]")
	fmt.Println("  help                          Show this help")
	fmt.Println("  connect <dsn>                 Connect to a database and create a named session")
	fmt.Println("  list drivers                  List JDBC drivers loaded by backend")
	fmt.Println("  reload drivers                Rescan and reload JDBC drivers on backend")
	fmt.Println("  detach                        Leave REPL without disconnecting (like tmux detach)")
	fmt.Println("  exit | quit                   Disconnect backend session and remove it from registry")
	fmt.Println("  set display wide|narrow       Toggle truncation mode for tabular output")
	fmt.Println("  set display expanded on|off   Expanded display mode")
	fmt.Println("  set display width <n>         Set max column width for tabular output")
	fmt.Println("  set dbtype <dbtype>           Set dbType for /ai in empty REPL (before connect)")
	fmt.Println("  set output table|csv|tsv|json Set output format")
	fmt.Println("")
	fmt.Println("[psql-compat (\\)]")
	fmt.Println("  \\conninfo                     Show current session and backend information")
	fmt.Println("  \\d <name>                     Describe a table/view (alias: desc)")
	fmt.Println("  \\d+ <name>                    Describe with more details (alias: desc+)")
	fmt.Println("  \\dt | \\dv                     List tables/views")
	fmt.Println("  \\explain <sql>                Show execution plan (alias: explain, explain plan for)")
	fmt.Println("  \\explain analyze <sql>        Show actual execution plan (executes the statement) (alias: explain analyze)")
	fmt.Println("  \\sqltext <sql_id>             Get SQL text by ID")
	fmt.Println("  \\plan <sql_id>               Get execution plan for SQL by ID")
	fmt.Println("  \\top                          Show top performance metrics")
	fmt.Println("  \\watch <command>             Repeatedly execute a command (e.g., \\watch \\top)")
	fmt.Println("  \\i <file>                     Execute statements from a file (alias: @<file>)")
	fmt.Println("  \\x [on|off]                   Expanded display mode (same as set display expanded on|off)")
	fmt.Println("  \\o <file>                     Redirect query output to a file")
	fmt.Println("  \\o                            Restore output to stdout")
	fmt.Println("")
	fmt.Println("[AI (/)]")
	fmt.Println("  /ai <prompt>                  Generate SQL via AI and confirm before execution")
	fmt.Println("  /context show                 Show recent executed SQL context used by AI")
	fmt.Println("  /context clear                Clear AI context")
	fmt.Println("")
	fmt.Println("Notes:")
	fmt.Println("  - End a statement with ';' to execute")
	return true
}

// handleReplDetachExit handles detach/exit/quit commands.
func handleReplDetachExit(
	cmd *cobra.Command,
	line *liner.State,
	historyMode string,
	input string,
	c *client.Client,
	sessionID string,
	name string,
	cfg *config.Config,
) (handled bool, shouldBreak bool) {
	_ = cmd

	lower := strings.ToLower(strings.TrimSpace(input))
	if lower != "detach" && lower != "exit" && lower != "quit" {
		return false, false
	}
	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	if lower == "detach" {
		return true, true
	}

	if strings.TrimSpace(sessionID) == "" {
		// Empty REPL: no DB session to disconnect.
		return true, true
	}

	if err := c.Disconnect(sessionID); err != nil {
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
			if err := config.SaveConfig(cfg); err != nil {
				fmt.Printf("Warning: could not save config: %v\n", err)
			}
		}
	}

	return true, true
}

// handleReplSetDisplay handles "set display ..." commands.
func handleReplSetDisplay(line *liner.State, historyMode string, input string, cfg *config.Config) bool {
	lower := strings.ToLower(strings.TrimSpace(input))
	if !strings.HasPrefix(lower, "set display ") {
		return false
	}
	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	args := strings.Fields(lower)
	if len(args) == 3 {
		switch args[2] {
		case "wide":
			setDisplayWide(true)
			if cfg != nil {
				cfg.DisplayWide = true
				if err := config.SaveConfig(cfg); err != nil {
					fmt.Printf("Warning: could not save config: %v\n", err)
				}
			}
			fmt.Println("Display mode set to wide.")
			return true
		case "narrow":
			setDisplayWide(false)
			if cfg != nil {
				cfg.DisplayWide = false
				if err := config.SaveConfig(cfg); err != nil {
					fmt.Printf("Warning: could not save config: %v\n", err)
				}
			}
			fmt.Println("Display mode set to narrow.")
			return true
		}
	}

	if len(args) == 4 && args[2] == "expanded" {
		switch args[3] {
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
		}
	}

	if len(args) == 4 && args[2] == "width" {
		w, err := parseDisplayWidthArg(args[3])
		if err != nil {
			fmt.Println("Error: invalid width")
			return true
		}
		setDisplayWidth(w)
		if cfg != nil {
			cfg.Display.MaxColWidth = displayMaxColWidth
			if err := config.SaveConfig(cfg); err != nil {
				fmt.Printf("Warning: could not save config: %v\n", err)
			}
		}
		fmt.Printf("Display column width set to %d.\n", displayMaxColWidth)
		return true
	}

	fmt.Println("Usage: set display wide|narrow|expanded on|off|width <n>")
	return true
}

// handleReplSetOutput handles "set output ..." commands.
func handleReplSetOutput(line *liner.State, historyMode string, input string, cfg *config.Config) bool {
	lower := strings.ToLower(strings.TrimSpace(input))
	if !strings.HasPrefix(lower, "set output ") {
		return false
	}
	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	args := strings.Fields(lower)
	if len(args) == 3 {
		if err := setOutputFormat(args[2]); err != nil {
			fmt.Printf("Error: %v\n", err)
			return true
		}
		if cfg != nil {
			cfg.OutputFormat = strings.ToLower(strings.TrimSpace(args[2]))
			if err := config.SaveConfig(cfg); err != nil {
				fmt.Printf("Warning: could not save config: %v\n", err)
			}
		}
		fmt.Printf("Output format set to %s.\n", strings.ToLower(strings.TrimSpace(args[2])))
		return true
	}

	fmt.Println("Usage: set output table|csv|tsv|json")
	return true
}

func handleReplSetDbType(cmd *cobra.Command, line *liner.State, historyMode string, input string, c *client.Client, sessionId string, currentDbType *string) bool {
	_ = cmd
	trimmed := strings.TrimSpace(input)
	lower := strings.ToLower(trimmed)
	if !strings.HasPrefix(lower, "set dbtype ") {
		return false
	}
	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	if strings.TrimSpace(sessionId) != "" {
		fmt.Println("Error: already connected. dbType automatically follows current session.")
		return true
	}

	parts := strings.Fields(trimmed)
	if len(parts) != 3 {
		fmt.Println("Usage: set dbtype <dbtype>")
		return true
	}
	chosen := strings.ToLower(strings.TrimSpace(parts[2]))
	if chosen == "postgresql" {
		chosen = "postgres"
	}
	if chosen == "" {
		fmt.Println("Usage: set dbtype <dbtype>")
		return true
	}

	drivers, err := c.MetaDrivers()
	if err != nil {
		fmt.Printf("%v\n", err)
		return true
	}
	if !drivers.HasDbType(chosen) {
		fmt.Printf("Error: unknown dbType %q. Use 'list drivers' to see available dbTypes.\n", chosen)
		return true
	}

	*currentDbType = chosen
	fmt.Printf("dbType set to %s.\n", chosen)
	return true
}

// handleReplWatch handles the \watch command to repeatedly execute a command.
func handleReplWatch(
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

	if cmdLower != "\\watch" {
		return false
	}

	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	if len(args) < 1 {
		fmt.Println("Usage: \\watch <command>")
		return true
	}

	watchCommand := strings.Join(args, " ")
	interval := 10 * time.Second

	// Try to get interval from snapshot if available
	if snapshot, err := c.GetTopSnapshot(sessionId); err == nil && snapshot.IntervalSec > 0 {
		interval = time.Duration(snapshot.IntervalSec) * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("Watching: %s (interval: %v, Ctrl+C to stop)\n\n", watchCommand, interval)

	// Execute immediately first
	fmt.Print("\033[H\033[2J")
	executeWatchCommand(cmd, line, historyMode, watchCommand, c, sessionId, cfg)

	// Start ticker for subsequent executions
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Clear screen (platform-independent)
			fmt.Print("\033[H\033[2J")

			// Execute the watched command
			executeWatchCommand(cmd, line, historyMode, watchCommand, c, sessionId, cfg)

		case <-sigChan:
			fmt.Println("\nWatch stopped")
			return true

		case <-ctx.Done():
			return true
		}
	}
}

// executeWatchCommand executes a command in watch mode.
func executeWatchCommand(
	cmd *cobra.Command,
	line *liner.State,
	historyMode string,
	input string,
	c *client.Client,
	sessionId string,
	cfg *config.Config,
) {
	// Parse the command
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return
	}

	cmdName := parts[0]
	var args []string
	if len(parts) > 1 {
		args = parts[1:]
	}

	// Try to execute through the various command handlers
	// Check meta commands first
	if strings.HasPrefix(cmdName, "\\") {
		if handleReplMetaCommands(cmd, line, historyMode, input, cmdName, args, c, sessionId, cfg) {
			return
		}
		if handleReplTopCommands(cmd, line, historyMode, input, cmdName, args, c, sessionId, cfg) {
			return
		}
	} else {
		// Try to match without backslash for convenience (e.g., "top" -> "\top")
		fullCmdName := "\\" + cmdName
		if handleReplMetaCommands(cmd, line, historyMode, input, fullCmdName, args, c, sessionId, cfg) {
			return
		}
		if handleReplTopCommands(cmd, line, historyMode, input, fullCmdName, args, c, sessionId, cfg) {
			return
		}
	}

	// Check other CLI commands
	if handled, _ := handleReplDetachExit(cmd, line, historyMode, input, c, sessionId, "", cfg); handled {
		return
	}
	if handleReplSetDisplay(line, historyMode, input, cfg) {
		return
	}
	if handleReplSetOutput(line, historyMode, input, cfg) {
		return
	}

	// If it's a SQL statement, try to execute it
	if strings.Contains(input, ";") {
		// SQL statements are executed in the main REPL loop
		// In watch mode, we can't easily execute them without the full context
		fmt.Printf("SQL execution in watch mode is not supported\n")
		return
	}

	fmt.Printf("Unknown command in watch mode: %s\n", cmdName)
}
