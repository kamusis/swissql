package cmd

import (
	"fmt"
	"sort"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/spf13/cobra"
)

var lsCmd = &cobra.Command{
	Use:   "ls",
	Short: "List named sessions",
	RunE: func(cmd *cobra.Command, args []string) error {
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
		for _, name := range names {
			e := reg.Sessions[name]
			marker := " "
			if cfg.CurrentName != "" && cfg.CurrentName == name {
				marker = "*"
			}
			fmt.Printf("%s %s\t%s\t%s\t%s\n", marker, e.Name, e.DbType, e.ServerURL, e.SessionId)
		}
		return nil
	},
}

var attachCmd = &cobra.Command{
	Use:   "attach [name]",
	Short: "Attach to a named session and start REPL",
	Args:  cobra.RangeArgs(0, 1),
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
			name = cfg.CurrentName
		} else {
			mostRecent, ok := reg.MostRecentSessionName()
			if !ok {
				return fmt.Errorf("no sessions in registry. Use 'swissql connect --name <name> <dsn>' first")
			}
			name = mostRecent
		}

		cfg.CurrentName = name
		_ = config.SaveConfig(cfg)
		_ = config.TouchSession(name)

		replCmd.Flags().Set("name", name)
		return replCmd.RunE(replCmd, nil)
	},
}

var attachAliasCmd = &cobra.Command{
	Use:   "a [name]",
	Short: "Alias for attach",
	Args:  cobra.RangeArgs(0, 1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return attachCmd.RunE(attachCmd, args)
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
			_ = config.SaveConfig(cfg)
		}

		fmt.Printf("Killed session: %s\n", name)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(lsCmd)
	rootCmd.AddCommand(attachCmd)
	rootCmd.AddCommand(attachAliasCmd)
	rootCmd.AddCommand(killCmd)
}
