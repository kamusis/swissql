package cmd

import (
	"fmt"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "swissql",
	Short: "SwissQL is a database 'Swiss Army Knife' CLI",
	Long: `A unified, modern, AI-driven cross-database interactive entry point
	for DBAs, developers, DevOps, and data analysts.

Run without a subcommand to start an interactive REPL (empty until you connect).`,
	Example:       "swissql\n  swissql connect <dsn>\n  swissql attach [name]",
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		server, _ := cmd.Flags().GetString("server")
		timeoutMs, _ := cmd.Flags().GetInt("connection-timeout")

		c := client.NewClient(server, time.Duration(timeoutMs)*time.Millisecond)
		if err := c.Status(); err != nil {
			return fmt.Errorf("failed to connect to backend %s: %w", server, err)
		}

		return runRepl(cmd, args)
	},
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Server configuration
	rootCmd.PersistentFlags().StringP(
		"server", "s", "http://localhost:8080",
		"Backend server URL",
	)

	// AI & confirmation flags (global)
	rootCmd.PersistentFlags().Bool(
		"use-mcp", false,
		"Use MCP server for SQL execution instead of direct JDBC connection.",
	)
	rootCmd.PersistentFlags().BoolP(
		"yes", "y", false,
		"Skip confirmation prompts for AI-generated SQL execution.",
	)
	rootCmd.PersistentFlags().Bool(
		"password-stdin", false,
		"Read password from stdin instead of command line.",
	)

	// Timeout configuration (global)
	rootCmd.PersistentFlags().Int(
		"connection-timeout", 5000,
		"Connection timeout in milliseconds.",
	)

	// Output configuration (global)
	rootCmd.PersistentFlags().Bool(
		"plain", false,
		"Use plain ASCII output instead of Unicode box-drawing characters.",
	)
}
