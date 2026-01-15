package cmd

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

// replHelpItem represents a single help row in the REPL help output.
type replHelpItem struct {
	Group       string
	Command     string
	Description string
}

// replCommand represents a single REPL command entry.
//
// A command can match either by a custom Match function (for non-meta commands like "connect")
// or by matching MetaCmdName against Names (for meta commands parsed from isMetaCommandStart).
type replCommand struct {
	Names     []string
	Group     string
	Help      replHelpItem
	HelpItems func() []replHelpItem
	Match     func(input string, lower string) bool
	Run       func(ctx *replDispatchContext) (handled bool, shouldBreak bool)
	Requires  bool
}

// replDispatchContext carries the current REPL state and dependencies needed to execute commands.
type replDispatchContext struct {
	Cmd         *cobra.Command
	Line        *liner.State
	HistoryMode string
	Input       string
	Lower       string
	MetaCmdName string
	MetaArgs    []string
	Client      *client.Client

	SessionId      *string
	Entry          *config.SessionEntry
	Name           *string
	Cfg            *config.Config
	CurrentDbType  *string
	MultiLineSql   *[]string
	WatchMode      bool
	InvalidateFunc func()
	CompleterFunc  func(*client.Client, string) liner.Completer
}

// replHelpItems returns help rows for all registered commands.
func replHelpItems() []replHelpItem {
	items := []replHelpItem{}
	for _, c := range replRegistry() {
		if c.HelpItems != nil {
			items = append(items, c.HelpItems()...)
			continue
		}
		if strings.TrimSpace(c.Help.Command) == "" {
			continue
		}
		items = append(items, c.Help)
	}
	return items
}

// printReplHelp renders REPL help to the provided writer.
func printReplHelp(w io.Writer) {
	items := replHelpItems()
	sort.Slice(items, func(i, j int) bool {
		gi := strings.ToLower(strings.TrimSpace(items[i].Group))
		gj := strings.ToLower(strings.TrimSpace(items[j].Group))
		if gi != gj {
			return gi < gj
		}
		return strings.ToLower(items[i].Command) < strings.ToLower(items[j].Command)
	})

	tw := tabwriter.NewWriter(w, 0, 8, 2, ' ', 0)
	fmt.Fprintln(tw, "Commands:")
	fmt.Fprintln(tw, "")

	currentGroup := ""
	for _, it := range items {
		if it.Group != currentGroup {
			currentGroup = it.Group
			fmt.Fprintf(tw, "[%s]\n", currentGroup)
		}
		fmt.Fprintf(tw, "  %s\t%s\n", it.Command, it.Description)
	}

	fmt.Fprintln(tw, "")
	fmt.Fprintln(tw, "Notes:")
	fmt.Fprintln(tw, "  - End a statement with ';' to execute")
	fmt.Fprintln(tw, "  - Samplers do not auto-start on connect. Example:")
	fmt.Fprintln(tw, "      connect <dsn>")
	fmt.Fprintln(tw, "      \\sampler start top")
	fmt.Fprintln(tw, "      \\top")
	_ = tw.Flush()
}

// replRegistry returns the REPL command registry.
//
// This registry is intended to be the single source of truth for both help and dispatch.
func replRegistry() []replCommand {
	return []replCommand{
		{
			Names: []string{"help"},
			Group: "CLI",
			Help:  replHelpItem{Group: "CLI", Command: "help", Description: "Show this help"},
			Match: func(input string, lower string) bool {
				return strings.EqualFold(strings.TrimSpace(input), "help")
			},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				if shouldRecordHistory(ctx.HistoryMode, ctx.Input, false) {
					ctx.Line.AppendHistory(ctx.Input)
				}
				printReplHelp(getOutputWriter())
				return true, false
			},
		},
		{
			Names: []string{"connect"},
			Group: "CLI",
			Help:  replHelpItem{Group: "CLI", Command: "connect <dsn>", Description: "Connect to a database and create a named session"},
			Match: func(input string, lower string) bool {
				fields := strings.Fields(strings.TrimSpace(input))
				return len(fields) >= 2 && strings.EqualFold(fields[0], "connect")
			},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				connected, newEntry, newName := handleReplConnectCommand(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Client)
				if !connected {
					return false, false
				}
				if strings.TrimSpace(newEntry.SessionId) == "" {
					return true, false
				}

				if ctx.SessionId != nil && strings.TrimSpace(*ctx.SessionId) != "" {
					if err := ctx.Client.Disconnect(*ctx.SessionId); err != nil {
						fmt.Printf("Warning: failed to disconnect current session: %v\n", err)
					}

					if ctx.Name != nil && strings.TrimSpace(*ctx.Name) != "" {
						reg, err := config.LoadRegistry()
						if err == nil {
							reg.RemoveSession(*ctx.Name)
							_ = config.SaveRegistry(reg)
						}

						cfg, err := config.LoadConfig()
						if err == nil && cfg != nil && cfg.CurrentName == *ctx.Name {
							cfg.CurrentName = ""
							_ = config.SaveConfig(cfg)
						}
					}
				}

				if ctx.SessionId != nil {
					*ctx.SessionId = newEntry.SessionId
				}
				if ctx.Entry != nil {
					*ctx.Entry = newEntry
				}
				if ctx.CurrentDbType != nil {
					*ctx.CurrentDbType = newEntry.DbType
				}
				if ctx.Name != nil {
					*ctx.Name = newName
				}
				if ctx.Line != nil && ctx.CompleterFunc != nil {
					ctx.Line.SetCompleter(ctx.CompleterFunc(ctx.Client, newEntry.SessionId))
				}
				if ctx.InvalidateFunc != nil {
					ctx.InvalidateFunc()
				}
				fmt.Printf("Connected successfully! Session ID: %s\n", newEntry.SessionId)
				return true, false
			},
		},
		{
			Names: []string{"list drivers", "list driver"},
			Group: "CLI",
			Help:  replHelpItem{Group: "CLI", Command: "list drivers", Description: "List JDBC drivers loaded by backend"},
			Match: func(input string, lower string) bool {
				return strings.EqualFold(lower, "list drivers") || strings.EqualFold(lower, "list driver")
			},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplDriverCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Client), false
			},
		},
		{
			Names: []string{"connmgr"},
			Group: "CLI",
			HelpItems: func() []replHelpItem {
				return []replHelpItem{
					{Group: "CLI", Command: "connmgr import -dbp <file> [--conn_prefix <prefix>] [--on_conflict <strategy>] [--dry_run]", Description: "Import DBeaver project connections"},
					{Group: "CLI", Command: "connmgr list [--filter key=value ...]", Description: "List connection profiles"},
					{Group: "CLI", Command: "connmgr remove <name> [--force]", Description: "Remove connection profile"},
					{Group: "CLI", Command: "connmgr show <name>", Description: "Show profile details"},
					{Group: "CLI", Command: "connmgr update <name> [--new-name <name>] [--dsn <dsn>] [--db-type <type>]", Description: "Update profile properties"},
				}
			},
			Match: func(input string, lower string) bool {
				return strings.HasPrefix(lower, "connmgr ")
			},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				// Parse connmgr subcommand
				fields := strings.Fields(strings.TrimSpace(ctx.Input))
				if len(fields) < 2 {
					fmt.Println("Error: connmgr requires a subcommand (import, list, remove, show, update)")
					return true, false
				}

				subcommand := fields[1]
				ctx.MetaArgs = fields[2:]

				switch strings.ToLower(subcommand) {
				case "import":
					// Reset global variables to default values to avoid state pollution
					dbpPath = ""
					connPrefix = ""
					onConflict = "skip"
					dryRun = false

					// Parse flags and validate
					validFlags := map[string]bool{
						"-dbp":          true,
						"--conn_prefix": true,
						"--on_conflict": true,
						"--dry_run":     true,
					}

					for i := 2; i < len(fields); i++ {
						flag := fields[i]
						if !validFlags[flag] {
							fmt.Printf("Error: unknown flag '%s'\n", flag)
							fmt.Println("Available flags: -dbp, --conn_prefix, --on_conflict, --dry_run")
							return true, false
						}

						switch flag {
						case "-dbp":
							if i+1 >= len(fields) {
								fmt.Println("Error: -dbp requires a value")
								return true, false
							}
							dbpPath = fields[i+1]
							i++
						case "--conn_prefix":
							if i+1 >= len(fields) {
								fmt.Println("Error: --conn_prefix requires a value")
								return true, false
							}
							connPrefix = fields[i+1]
							i++
						case "--on_conflict":
							if i+1 >= len(fields) {
								fmt.Println("Error: --on_conflict requires a value")
								return true, false
							}
							onConflict = fields[i+1]
							i++
						case "--dry_run":
							dryRun = true
						}
					}

					if dbpPath == "" {
						fmt.Println("Error: -dbp flag is required for import")
						return true, false
					}

					return runConnmgrImport(ctx)
				case "list":
					return runConnmgrList(ctx)
				case "remove":
					return runConnmgrRemove(ctx)
				case "show":
					return runConnmgrShow(ctx)
				case "update":
					return runConnmgrUpdate(ctx)
				default:
					fmt.Printf("Error: unknown connmgr subcommand '%s'\n", subcommand)
					fmt.Println("Available subcommands: import, list, remove, show, update")
					return true, false
				}
			},
		},
		{
			Names: []string{"reload drivers", "reload driver"},
			Group: "CLI",
			Help:  replHelpItem{Group: "CLI", Command: "reload drivers", Description: "Rescan and reload JDBC drivers on backend"},
			Match: func(input string, lower string) bool {
				return strings.EqualFold(lower, "reload drivers") || strings.EqualFold(lower, "reload driver")
			},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplDriverCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Client), false
			},
		},
		{
			Names: []string{"detach"},
			Group: "CLI",
			Help:  replHelpItem{Group: "CLI", Command: "detach", Description: "Leave REPL without disconnecting (like tmux detach)"},
			Match: func(input string, lower string) bool { return lower == "detach" },
			Run: func(ctx *replDispatchContext) (bool, bool) {
				handled, shouldBreak := handleReplDetachExit(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Client, derefStr(ctx.SessionId), derefStr(ctx.Name), ctx.Cfg)
				return handled, shouldBreak
			},
		},
		{
			Names: []string{"exit", "quit"},
			Group: "CLI",
			Help:  replHelpItem{Group: "CLI", Command: "exit | quit", Description: "Disconnect backend session and remove it from registry"},
			Match: func(input string, lower string) bool { return lower == "exit" || lower == "quit" },
			Run: func(ctx *replDispatchContext) (bool, bool) {
				handled, shouldBreak := handleReplDetachExit(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Client, derefStr(ctx.SessionId), derefStr(ctx.Name), ctx.Cfg)
				return handled, shouldBreak
			},
			Requires: false,
		},
		{
			Names: []string{"set display"},
			Group: "CLI",
			Help:  replHelpItem{Group: "CLI", Command: "set display ...", Description: "Configure display options (wide/narrow/expanded/width)"},
			Match: func(input string, lower string) bool { return strings.HasPrefix(lower, "set display ") },
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplSetDisplay(ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Cfg), false
			},
		},
		{
			Names: []string{"set output"},
			Group: "CLI",
			Help:  replHelpItem{Group: "CLI", Command: "set output table|csv|tsv|json", Description: "Set output format"},
			Match: func(input string, lower string) bool { return strings.HasPrefix(lower, "set output ") },
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplSetOutput(ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Cfg), false
			},
		},
		{
			Names: []string{"set dbtype"},
			Group: "CLI",
			Help:  replHelpItem{Group: "CLI", Command: "set dbtype <dbtype>", Description: "Set dbType for /ai in empty REPL (before connect)"},
			Match: func(input string, lower string) bool { return strings.HasPrefix(lower, "set dbtype ") },
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplSetDbType(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Client, derefStr(ctx.SessionId), ctx.CurrentDbType), false
			},
		},

		{
			Names: []string{"/context"},
			Group: "AI",
			Help:  replHelpItem{Group: "AI", Command: "/context show", Description: "Show recent executed SQL context used by AI"},
			Match: func(input string, lower string) bool { return lower == "/context show" },
			Run: func(ctx *replDispatchContext) (bool, bool) {
				if strings.TrimSpace(derefStr(ctx.SessionId)) == "" {
					fmt.Println("Error: no active DB session. Use 'connect <dsn>' first.")
					return true, false
				}
				return handleReplContextCommands(ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Lower, ctx.Client, derefStr(ctx.SessionId)), false
			},
			Requires: true,
		},
		{
			Names: []string{"/context"},
			Group: "AI",
			Help:  replHelpItem{Group: "AI", Command: "/context clear", Description: "Clear AI context"},
			Match: func(input string, lower string) bool { return lower == "/context clear" },
			Run: func(ctx *replDispatchContext) (bool, bool) {
				if strings.TrimSpace(derefStr(ctx.SessionId)) == "" {
					fmt.Println("Error: no active DB session. Use 'connect <dsn>' first.")
					return true, false
				}
				return handleReplContextCommands(ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Lower, ctx.Client, derefStr(ctx.SessionId)), false
			},
			Requires: true,
		},
		{
			Names: []string{"/ai"},
			Group: "AI",
			Help:  replHelpItem{Group: "AI", Command: "/ai <prompt>", Description: "Generate SQL via AI and confirm before execution"},
			Match: func(input string, lower string) bool { return strings.HasPrefix(lower, "/ai ") },
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplAICommand(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.Client, derefStr(ctx.SessionId), derefStr(ctx.CurrentDbType), ctx.MultiLineSql), false
			},
		},

		{
			Names: []string{"\\conninfo", "conninfo"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\conninfo", Description: "Show current session and backend information"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplMetaCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId), ctx.Cfg), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\d", "\\d+", "desc", "desc+"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\d <name>", Description: "Describe a table/view (alias: desc)"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplMetaCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId), ctx.Cfg), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\dt", "\\dv"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\dt | \\dv", Description: "List tables/views"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplMetaCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId), ctx.Cfg), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\explain", "explain"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\explain <sql>", Description: "Show execution plan (alias: explain, explain plan for)"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplMetaCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId), ctx.Cfg), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\x"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\x [on|off]", Description: "Expanded display mode"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplMetaCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId), ctx.Cfg), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\top"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\top", Description: "Show top performance metrics"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplTopCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId), ctx.Cfg), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\sampler"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\sampler <action> <sampler>", Description: "Control samplers (explicit)"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplSamplerCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId), ctx.Cfg), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\swiss"},
			Group: "psql-compat (\\)",
			HelpItems: func() []replHelpItem {
				out := make([]replHelpItem, 0, len(swissUsage))
				for _, l := range swissUsage {
					out = append(out, replHelpItem{Group: "psql-compat (\\)", Command: l.Command, Description: l.Description})
				}
				return out
			},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplSwissCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId), ctx.Cfg), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\watch"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\watch <command>", Description: "Repeatedly execute a command"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplWatch(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId), ctx.Cfg), false
			},
		},
		{
			Names: []string{"\\i", "@"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\i <file>", Description: "Execute statements from a file (alias: @<file>)"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplIOCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId)), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\o"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\o <file>", Description: "Redirect query output to a file"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplIOCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId)), false
			},
			Requires: true,
		},
		{
			Names: []string{"\\o"},
			Group: "psql-compat (\\)",
			Help:  replHelpItem{Group: "psql-compat (\\)", Command: "\\o", Description: "Restore output to stdout"},
			Run: func(ctx *replDispatchContext) (bool, bool) {
				return handleReplIOCommands(ctx.Cmd, ctx.Line, ctx.HistoryMode, ctx.Input, ctx.MetaCmdName, ctx.MetaArgs, ctx.Client, derefStr(ctx.SessionId)), false
			},
			Requires: true,
		},
	}
}

// derefStr returns the value of a string pointer or "" if nil.
func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// dispatchReplLine executes a non-meta line using the registry.
func dispatchReplLine(ctx *replDispatchContext) (handled bool, shouldBreak bool) {
	if ctx == nil {
		return false, false
	}

	for _, c := range replRegistry() {
		if c.Match != nil {
			if !c.Match(ctx.Input, ctx.Lower) {
				continue
			}
		} else if len(c.Names) > 0 {
			matched := false
			for _, n := range c.Names {
				if strings.EqualFold(strings.TrimSpace(n), strings.TrimSpace(ctx.MetaCmdName)) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		if c.Requires && strings.TrimSpace(derefStr(ctx.SessionId)) == "" {
			fmt.Println("Error: no active DB session. Use 'connect <dsn>' first.")
			return true, false
		}

		if c.Run == nil {
			return false, false
		}
		return c.Run(ctx)
	}

	return false, false
}

// dispatchReplMeta parses a meta command line and dispatches it using the registry.
func dispatchReplMeta(ctx *replDispatchContext) (handled bool, shouldBreak bool) {
	if ctx == nil {
		return false, false
	}

	if !isMetaCommandStart(ctx.Input) {
		return false, false
	}

	cmdName, args := parseMetaCommand(ctx.Input)
	ctx.MetaCmdName = cmdName
	ctx.MetaArgs = args

	// Support meta aliases without leading backslash in watch mode and user input.
	if !strings.HasPrefix(cmdName, "\\") {
		lowerName := strings.ToLower(strings.TrimSpace(cmdName))
		switch lowerName {
		case "top", "sampler", "swiss", "watch", "i", "o", "x", "dt", "dv", "d", "d+":
			ctx.MetaCmdName = "\\" + lowerName
		case "desc", "desc+", "explain", "conninfo":
			// keep as-is; handled by existing meta handler
		default:
		}
	}

	return dispatchReplLine(ctx)
}
