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
	"golang.org/x/term"
)

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

	// Try to get interval from sampler snapshot if available
	if snapshot, err := c.SamplerSnapshot(sessionId, "top"); err == nil && snapshot.IntervalSec != nil && *snapshot.IntervalSec > 0 {
		interval = time.Duration(*snapshot.IntervalSec) * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err == nil {
		defer func() {
			_ = term.Restore(int(os.Stdin.Fd()), oldState)
		}()
	}

	quitChan := make(chan struct{}, 1)
	go func() {
		buf := make([]byte, 1)
		for {
			n, readErr := os.Stdin.Read(buf)
			if readErr != nil {
				return
			}
			if n == 1 {
				b := buf[0]
				if b == 'q' || b == 'Q' {
					quitChan <- struct{}{}
					return
				}
			}
		}
	}()

	fmt.Printf("Watching: %s (interval: %v, Ctrl+C or 'q' to stop)\n\n", watchCommand, interval)

	// Execute immediately first
	clearScreen()
	executeWatchCommand(cmd, line, historyMode, watchCommand, c, sessionId, cfg)

	// Start ticker for subsequent executions
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Clear screen (platform-independent)
			clearScreen()

			// Execute the watched command
			executeWatchCommand(cmd, line, historyMode, watchCommand, c, sessionId, cfg)

		case <-sigChan:
			fmt.Println("\nWatch stopped")
			return true

		case <-quitChan:
			fmt.Println("\nWatch stopped")
			return true

		case <-ctx.Done():
			return true
		}
	}
}

func clearScreen() {
	fmt.Print("\033[2J\033[H")
}

// shouldAutoPrefixMetaInWatch returns true if the given input looks like a registered REPL meta
// command that is missing its leading backslash. This is intentionally driven off the REPL
// registry so new meta commands do not require changes here.
func shouldAutoPrefixMetaInWatch(input string) bool {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return false
	}
	if strings.HasPrefix(trimmed, "\\") || strings.HasPrefix(trimmed, "@") {
		return false
	}
	// SQL execution in watch mode is not supported; do not rewrite SQL-like input.
	if strings.Contains(trimmed, ";") {
		return false
	}

	parts := strings.Fields(trimmed)
	if len(parts) == 0 {
		return false
	}
	first := strings.ToLower(parts[0])

	for _, c := range replRegistry() {
		for _, n := range c.Names {
			name := strings.TrimSpace(n)
			if !strings.HasPrefix(name, "\\") {
				continue
			}
			metaToken := strings.ToLower(strings.TrimPrefix(name, "\\"))
			if metaToken == first {
				return true
			}
		}
	}

	return false
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
	normalizedInput := strings.TrimSpace(input)

	// Auto-prefix backslash for meta commands without it (e.g., "top" -> "\top")
	if shouldAutoPrefixMetaInWatch(normalizedInput) {
		normalizedInput = "\\" + normalizedInput
	}

	dispatchCtx := &replDispatchContext{
		Cmd:         cmd,
		Line:        line,
		HistoryMode: historyMode,
		Input:       normalizedInput,
		Lower:       strings.ToLower(normalizedInput),
		Client:      c,
		SessionId:   &sessionId,
		Cfg:         cfg,
		WatchMode:   true,
	}

	if handled, _ := dispatchReplLine(dispatchCtx); handled {
		return
	}
	if handled, _ := dispatchReplMeta(dispatchCtx); handled {
		return
	}

	if strings.Contains(input, ";") {
		fmt.Printf("SQL execution in watch mode is not supported\n")
		return
	}

	parts := strings.Fields(strings.TrimSpace(input))
	if len(parts) == 0 {
		return
	}
	fmt.Printf("Unknown command in watch mode: %s\n", parts[0])
}
