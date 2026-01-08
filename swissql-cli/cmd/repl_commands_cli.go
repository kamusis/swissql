package cmd

import (
	"fmt"
	"strings"

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
	fmt.Println("  detach                        Leave REPL without disconnecting (like tmux detach)")
	fmt.Println("  exit | quit                   Disconnect backend session and remove it from registry")
	fmt.Println("  set display wide|narrow       Toggle truncation mode for tabular output")
	fmt.Println("  set display expanded on|off   Expanded display mode")
	fmt.Println("  set display width <n>         Set max column width for tabular output")
	fmt.Println("  set output table|csv|tsv|json Set output format")
	fmt.Println("")
	fmt.Println("[psql-compat (\\)]")
	fmt.Println("  \\conninfo                    Show current session and backend information")
	fmt.Println("  \\d <name> (alias: desc)       Describe a table/view")
	fmt.Println("  \\d+ <name> (alias: desc+)     Describe with more details")
	fmt.Println("  \\dt | \\dv                     List tables/views")
	fmt.Println("  \\explain <sql> (alias: explain, explain plan for)")
	fmt.Println("                               Show execution plan")
	fmt.Println("  \\explain analyze <sql> (alias: explain analyze)")
	fmt.Println("                               Show actual execution plan (executes the statement)")
	fmt.Println("  \\i <file> (alias: @<file>)    Execute statements from a file")
	fmt.Println("  \\x [on|off]                   Expanded display mode (like psql \\\\x)")
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
			_ = config.SaveConfig(cfg)
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
				_ = config.SaveConfig(cfg)
			}
			fmt.Println("Display mode set to wide.")
			return true
		case "narrow":
			setDisplayWide(false)
			if cfg != nil {
				cfg.DisplayWide = false
				_ = config.SaveConfig(cfg)
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
				_ = config.SaveConfig(cfg)
			}
			fmt.Println("Expanded display mode enabled.")
			return true
		case "off":
			setDisplayExpanded(false)
			if cfg != nil {
				cfg.DisplayExpanded = false
				_ = config.SaveConfig(cfg)
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
			_ = config.SaveConfig(cfg)
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
			_ = config.SaveConfig(cfg)
		}
		fmt.Printf("Output format set to %s.\n", strings.ToLower(strings.TrimSpace(args[2])))
		return true
	}

	fmt.Println("Usage: set output table|csv|tsv|json")
	return true
}
