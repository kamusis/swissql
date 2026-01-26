package cmd

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/spf13/cobra"
)

func truncateWithEllipsis(s string, width int) string {
	if width <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= width {
		return s + strings.Repeat(" ", width-len(r))
	}
	if width <= 3 {
		return string(r[:width])
	}
	return string(r[:width-3]) + "..."
}

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List named sessions",
	RunE: func(cmd *cobra.Command, args []string) error {
		prune, _ := cmd.Flags().GetBool("prune")
		reg, err := config.LoadRegistry()
		if err != nil {
			return err
		}

		names := make([]string, 0, len(reg.Sessions))
		for name := range reg.Sessions {
			names = append(names, name)
		}
		sort.Strings(names)

		if len(names) == 0 {
			fmt.Println("No sessions in registry. Use 'swissql connect --name <name> <dsn>' to create one.")
			return nil
		}

		cfg, _ := config.LoadConfig()
		if prune {
			timeout, _ := cmd.Flags().GetInt("connection-timeout")
			pruned := make([]string, 0)
			for _, name := range names {
				e := reg.Sessions[name]
				c := client.NewClient(e.ServerURL, time.Duration(timeout)*time.Millisecond)
				if err := c.Status(); err != nil {
					reg.RemoveSession(name)
					pruned = append(pruned, name)
					if cfg != nil && cfg.CurrentName == name {
						cfg.CurrentName = ""
					}
					continue
				}
				if err := c.ValidateSession(e.SessionId); err != nil {
					reg.RemoveSession(name)
					pruned = append(pruned, name)
					if cfg != nil && cfg.CurrentName == name {
						cfg.CurrentName = ""
					}
				}
			}
			if len(pruned) > 0 {
				if err := config.SaveRegistry(reg); err != nil {
					return err
				}
				if cfg != nil {
					if err := config.SaveConfig(cfg); err != nil {
						fmt.Printf("Warning: could not save config: %v\n", err)
					}
				}
				fmt.Printf("Pruned %d unreachable session(s): %s\n", len(pruned), strings.Join(pruned, ", "))
			}

			// Rebuild names after pruning
			names = make([]string, 0, len(reg.Sessions))
			for name := range reg.Sessions {
				names = append(names, name)
			}
			sort.Strings(names)
		}

		for _, name := range names {
			e := reg.Sessions[name]
			marker := " "
			if cfg.CurrentName != "" && cfg.CurrentName == name {
				marker = "*"
			}
			host := truncateWithEllipsis(e.GetRemoteHost(), 32)
			fmt.Printf("%s %-20s %-10s %-32s %s\n", marker, e.Name, e.DbType, host, e.SessionId)
		}
		return nil
	},
}

var attachCmd = &cobra.Command{
	Use:     "attach [name]",
	Short:   "Attach to a named session and start REPL",
	Aliases: []string{"a"},
	Args:    cobra.RangeArgs(0, 1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.LoadConfig()
		if err != nil {
			return err
		}
		reg, err := config.LoadRegistry()
		if err != nil {
			return err
		}

		name := ""
		if len(args) == 1 {
			name = args[0]
		} else if cfg.CurrentName != "" {
			// Validate that the session exists in registry
			if _, ok := reg.GetSession(cfg.CurrentName); ok {
				name = cfg.CurrentName
			} else {
				// Orphaned config, clear it and fall through to default logic
				cfg.CurrentName = ""
				_ = config.SaveConfig(cfg)
			}
		}

		if name == "" {
			mostRecent, ok := reg.MostRecentSessionName()
			if !ok {
				return fmt.Errorf("no sessions in registry. Use 'swissql connect --name <name> <dsn>' first")
			}
			name = mostRecent
		}

		cfg.CurrentName = name
		if err := config.SaveConfig(cfg); err != nil {
			fmt.Printf("Warning: could not save config: %v\n", err)
		}
		_ = config.TouchSession(name)

		replCmd.Flags().Set("name", name)
		return replCmd.RunE(replCmd, nil)
	},
}

var killCmd = &cobra.Command{
	Use:   "kill [name]",
	Short: "Kill a named session (disconnect backend and remove from registry)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		name := args[0]
		entry, err := config.ResolveActiveSession(name)
		if err != nil {
			return err
		}

		timeout, _ := cmd.Flags().GetInt("connection-timeout")
		c := client.NewClient(entry.ServerURL, time.Duration(timeout)*time.Millisecond)
		_ = c.Disconnect(entry.SessionId)

		reg, err := config.LoadRegistry()
		if err != nil {
			return err
		}
		reg.RemoveSession(name)
		if err := config.SaveRegistry(reg); err != nil {
			return err
		}

		cfg, _ := config.LoadConfig()
		if cfg.CurrentName == name {
			cfg.CurrentName = ""
			if err := config.SaveConfig(cfg); err != nil {
				fmt.Printf("Warning: could not save config: %v\n", err)
			}
		}

		fmt.Printf("Killed session: %s\n", name)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(lsCmd)
	lsCmd.Flags().Bool("prune", false, "Remove sessions whose backends are unreachable")
	rootCmd.AddCommand(attachCmd)
	rootCmd.AddCommand(killCmd)
}
