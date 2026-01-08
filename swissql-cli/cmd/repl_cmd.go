package cmd

import (
	"github.com/spf13/cobra"
)

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Start an interactive SQL REPL",
	RunE:  runRepl,
}

func init() {
	replCmd.Flags().String("name", "", "Session name to use (tmux-like)")
}
