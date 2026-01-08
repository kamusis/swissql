package cmd

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

func handleReplConnectCommand(cmd *cobra.Command, line *liner.State, historyMode string, input string, c *client.Client) (connected bool, entry config.SessionEntry, name string) {
	_ = cmd
	_ = line

	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return false, config.SessionEntry{}, ""
	}

	fields := strings.Fields(trimmed)
	if len(fields) < 2 {
		return false, config.SessionEntry{}, ""
	}

	if strings.ToLower(fields[0]) != "connect" {
		return false, config.SessionEntry{}, ""
	}

	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	dsn := strings.TrimSpace(strings.Trim(trimmed[len(fields[0]):], " \t"))
	dsn = strings.Trim(dsn, "\"'")
	if dsn == "" {
		fmt.Println("Error: connect requires a DSN")
		return true, config.SessionEntry{}, ""
	}

	dbType := "oracle" // Default
	if parsed, err := url.Parse(dsn); err == nil {
		if strings.TrimSpace(parsed.Scheme) != "" {
			dbType = strings.ToLower(parsed.Scheme)
		}
	}
	if dbType == "postgresql" {
		dbType = "postgres"
	}

	if dbType != "oracle" && dbType != "postgres" {
		drivers, err := c.MetaDrivers()
		if err != nil {
			fmt.Printf("%v\n", err)
			return true, config.SessionEntry{}, ""
		}
		if !drivers.HasDbType(dbType) {
			fmt.Printf("unknown dbType %q. Ensure the backend has loaded the JDBC driver and try again\n", dbType)
			return true, config.SessionEntry{}, ""
		}
	}

	req := &client.ConnectRequest{
		Dsn:    dsn,
		DbType: dbType,
		Options: client.ConnectOptions{
			ReadOnly:            false,
			UseMcp:              false,
			ConnectionTimeoutMs: int(c.Timeout.Milliseconds()),
		},
	}

	resp, err := c.Connect(req)
	if err != nil {
		fmt.Printf("%v\n", err)
		return true, config.SessionEntry{}, ""
	}

	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		fmt.Printf("failed to generate session name: %v\n", err)
		return true, config.SessionEntry{}, ""
	}
	name = fmt.Sprintf("%s-%s", dbType, hex.EncodeToString(buf))

	now := time.Now()
	entry = config.SessionEntry{
		Name:       name,
		SessionId:  resp.SessionId,
		ServerURL:  c.BaseURL,
		DbType:     dbType,
		DsnMasked:  config.MaskDsn(dsn),
		CreatedAt:  now,
		LastUsedAt: now,
	}

	reg, err := config.LoadRegistry()
	if err != nil {
		fmt.Printf("%v\n", err)
		return true, config.SessionEntry{}, ""
	}
	reg.UpsertSession(entry)
	if err := config.SaveRegistry(reg); err != nil {
		fmt.Printf("%v\n", err)
		return true, config.SessionEntry{}, ""
	}

	cfg, _ := config.LoadConfig()
	if cfg != nil {
		cfg.CurrentName = name
		if err := config.SaveConfig(cfg); err != nil {
			fmt.Printf("Warning: could not save config: %v\n", err)
		}
	}

	return true, entry, name
}
