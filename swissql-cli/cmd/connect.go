package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/spf13/cobra"
)

var connectCmd = &cobra.Command{
	Use:   "connect [DSN]",
	Short: "Connect to a database",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		dsn := args[0]
		server, _ := cmd.Flags().GetString("server")
		name, _ := cmd.Flags().GetString("name")
		readWrite, _ := cmd.Flags().GetBool("read-write")
		readOnly, _ := cmd.Flags().GetBool("read-only")
		timeout, _ := cmd.Flags().GetInt("connection-timeout")
		useMcp, _ := cmd.Flags().GetBool("use-mcp")

		dbType := "oracle" // Default
		if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
			dbType = "postgres"
		}

		fmt.Printf("Connecting to %s via backend %s...\n", dsn, server)

		// Default to read-write; only enable read-only if explicitly requested.
		resolvedReadOnly := false
		if cmd.Flags().Changed("read-only") {
			resolvedReadOnly = readOnly
		} else if cmd.Flags().Changed("read-write") {
			resolvedReadOnly = !readWrite
		}

		c := client.NewClient(server, time.Duration(timeout)*time.Millisecond)
		req := &client.ConnectRequest{
			Dsn:    dsn,
			DbType: dbType,
			Options: client.ConnectOptions{
				ReadOnly:            resolvedReadOnly,
				UseMcp:              useMcp,
				ConnectionTimeoutMs: timeout,
			},
		}

		resp, err := c.Connect(req)
		if err != nil {
			return err
		}

		fmt.Printf("Connected successfully! Session ID: %s\n", resp.SessionId)

		if name == "" {
			buf := make([]byte, 4)
			if _, err := rand.Read(buf); err != nil {
				return fmt.Errorf("failed to generate session name: %w", err)
			}
			name = fmt.Sprintf("%s-%s", dbType, hex.EncodeToString(buf))
		}

		// Save to registry (tmux-like sessions)
		reg, err := config.LoadRegistry()
		if err != nil {
			return err
		}
		now := time.Now()
		reg.UpsertSession(config.SessionEntry{
			Name:       name,
			SessionId:  resp.SessionId,
			ServerURL:  server,
			DbType:     dbType,
			DsnMasked:  config.MaskDsn(dsn),
			CreatedAt:  now,
			LastUsedAt: now,
		})
		if err := config.SaveRegistry(reg); err != nil {
			return err
		}

		// Save to config (keep legacy fields for backward compatibility)
		cfg, _ := config.LoadConfig()
		cfg.CurrentName = name
		if err := config.SaveConfig(cfg); err != nil {
			return fmt.Errorf("failed to save config: %w", err)
		}

		// Start REPL automatically
		return startRepl(cmd)
	},
}

func startRepl(cmd *cobra.Command) error {
	return replCmd.RunE(cmd, nil)
}

func init() {
	rootCmd.AddCommand(connectCmd)
	connectCmd.Flags().Bool("read-write", false, "Enable read-write mode")
	connectCmd.Flags().String("name", "", "Name this session (tmux-like)")
}
