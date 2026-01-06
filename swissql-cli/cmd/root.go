package cmd

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "swissql",
	Short: "SwissQL is a database 'Swiss Army Knife' CLI",
	Long: `A unified, modern, AI-driven cross-database interactive entry point
for DBAs, developers, DevOps, and data analysts.`,
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

	// Security flags (global - inherited by all subcommands)
	rootCmd.PersistentFlags().Bool(
		"read-only", true,
		"Execute queries in read-only mode (default: true)",
	)
	rootCmd.PersistentFlags().Bool(
		"read-write", false,
		"Allow write operations (DML/DDL). Requires backend permission.",
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
