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
	"github.com/kamusis/swissql/swissql-cli/internal/ui"
	"github.com/spf13/cobra"
)

var (
	profileName string
)

type pendingCredentialSave struct {
	Username      string
	Password      string
	ShouldSave    bool
	UpdateProfile bool
}

func inferDbTypeFromDsn(dsn string) string {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return config.NormalizeDbType("oracle")
	}
	if strings.TrimSpace(parsed.Scheme) == "" {
		return config.NormalizeDbType("oracle")
	}
	return config.NormalizeDbType(parsed.Scheme)
}

var connectCmd = &cobra.Command{
	Use:   "connect [DSN]",
	Short: "Connect to a database",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		server, _ := cmd.Flags().GetString("server")
		name, _ := cmd.Flags().GetString("name")
		timeout, _ := cmd.Flags().GetInt("connection-timeout")
		useMcp, _ := cmd.Flags().GetBool("use-mcp")
		disconnectCurrent, _ := cmd.Flags().GetBool("disconnect-current")

		var dsn string
		var dbType string

		// Profile mode
		if profileName != "" {
			profile, err := config.GetProfile(profileName)
			if err != nil {
				return fmt.Errorf("failed to load profile: %w", err)
			}
			if profile == nil {
				return fmt.Errorf("profile '%s' not found", profileName)
			}

			dbType = config.NormalizeDbType(profile.DBType)
			dsn, pending, err := buildDSNWithCredentials(profile)
			if err != nil {
				return fmt.Errorf("failed to build DSN: %w", err)
			}

			resp, err := connectWithDsn(cmd, server, timeout, useMcp, disconnectCurrent, name, dsn, dbType)
			if err != nil {
				return err
			}

			if pending != nil && pending.ShouldSave {
				if pending.UpdateProfile {
					profile.SavePassword = true
					if _, err := config.AddProfile(profileName, profile, config.ConflictOverwrite); err != nil {
						return fmt.Errorf("failed to update profile: %w", err)
					}
				}

				if err := config.SetCredentials(profile.ID, pending.Username, pending.Password); err != nil {
					return fmt.Errorf("failed to save credentials: %w", err)
				}
			}

			fmt.Printf("Connected successfully! Session ID: %s\n", resp.SessionId)
			return finalizeConnect(cmd, server, timeout, dbType, dsn, name, resp.SessionId)
		} else {
			// DSN mode (existing behavior)
			if len(args) == 0 {
				return fmt.Errorf("DSN is required when not using --profile")
			}
			dsn = args[0]
			dbType = inferDbTypeFromDsn(dsn)
		}

		resp, err := connectWithDsn(cmd, server, timeout, useMcp, disconnectCurrent, name, dsn, dbType)
		if err != nil {
			return err
		}

		fmt.Printf("Connected successfully! Session ID: %s\n", resp.SessionId)
		return finalizeConnect(cmd, server, timeout, dbType, dsn, name, resp.SessionId)
	},
}

func connectWithDsn(cmd *cobra.Command, server string, timeout int, useMcp bool, disconnectCurrent bool, name string, dsn string, dbType string) (*client.ConnectResponse, error) {
	fmt.Printf("Connecting to %s via backend %s...\n", dsn, server)

	c := client.NewClient(server, time.Duration(timeout)*time.Millisecond)

	if disconnectCurrent {
		if oldEntry, err := config.ResolveActiveSession(""); err == nil {
			oldClient := client.NewClient(oldEntry.ServerURL, time.Duration(timeout)*time.Millisecond)
			_ = oldClient.Disconnect(oldEntry.SessionId)

			reg, err := config.LoadRegistry()
			if err == nil {
				reg.RemoveSession(oldEntry.Name)
				_ = config.SaveRegistry(reg)
			}
			cfg, err := config.LoadConfig()
			if err == nil && cfg != nil && cfg.CurrentName == oldEntry.Name {
				cfg.CurrentName = ""
				_ = config.SaveConfig(cfg)
			}
		}
	}
	return connectWithClient(c, timeout, useMcp, dsn, dbType)
}

func connectWithClient(c *client.Client, timeout int, useMcp bool, dsn string, dbType string) (*client.ConnectResponse, error) {
	normalizedDbType := config.NormalizeDbType(dbType)
	if !config.IsBuiltinDbType(normalizedDbType) {
		drivers, err := c.MetaDrivers()
		if err != nil {
			return nil, err
		}
		if !drivers.HasDbType(normalizedDbType) {
			return nil, fmt.Errorf("unknown dbType %q. Ensure the backend has loaded the JDBC driver and try again", normalizedDbType)
		}
	}
	req := &client.ConnectRequest{
		Dsn:    dsn,
		DbType: normalizedDbType,
		Options: client.ConnectOptions{
			ReadOnly:            false,
			UseMcp:              useMcp,
			ConnectionTimeoutMs: timeout,
		},
	}

	resp, err := c.Connect(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func finalizeConnect(cmd *cobra.Command, server string, timeout int, dbType string, dsn string, name string, sessionId string) error {
	_, _, err := persistConnectedSession(server, dbType, dsn, name, sessionId)
	if err != nil {
		return err
	}
	return startRepl(cmd)
}

func persistConnectedSession(server string, dbType string, dsn string, name string, sessionId string) (config.SessionEntry, string, error) {
	normalizedDbType := config.NormalizeDbType(dbType)
	if name == "" {
		buf := make([]byte, 4)
		if _, err := rand.Read(buf); err != nil {
			return config.SessionEntry{}, "", fmt.Errorf("failed to generate session name: %w", err)
		}
		name = fmt.Sprintf("%s-%s", normalizedDbType, hex.EncodeToString(buf))
	}

	reg, err := config.LoadRegistry()
	if err != nil {
		return config.SessionEntry{}, "", err
	}
	now := time.Now()
	entry := config.SessionEntry{
		Name:       name,
		SessionId:  sessionId,
		ServerURL:  server,
		DbType:     normalizedDbType,
		DsnMasked:  config.MaskDsn(dsn),
		CreatedAt:  now,
		LastUsedAt: now,
	}
	reg.UpsertSession(entry)
	if err := config.SaveRegistry(reg); err != nil {
		return config.SessionEntry{}, "", err
	}

	cfg, _ := config.LoadConfig()
	cfg.CurrentName = name
	if err := config.SaveConfig(cfg); err != nil {
		return config.SessionEntry{}, "", fmt.Errorf("failed to save config: %w", err)
	}

	return entry, name, nil
}

func startRepl(cmd *cobra.Command) error {
	return replCmd.RunE(cmd, nil)
}

// buildDSNWithCredentials builds a DSN with credentials from profile
func buildDSNWithCredentials(profile *config.Profile) (string, *pendingCredentialSave, error) {
	// Try to load existing credentials
	username, password, err := config.GetCredentials(profile.ID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to load credentials: %w", err)
	}

	// If credentials don't exist, prompt user
	if username == "" && password == "" {
		username, err = ui.PromptUsername()
		if err != nil {
			return "", nil, fmt.Errorf("failed to prompt for username: %w", err)
		}

		password, err = ui.PromptPassword()
		if err != nil {
			return "", nil, fmt.Errorf("failed to prompt for password: %w", err)
		}

		pending := &pendingCredentialSave{Username: username, Password: password}
		if profile.SavePassword {
			pending.ShouldSave = true
		} else {
			save, err := ui.PromptSavePassword()
			if err != nil {
				return "", nil, fmt.Errorf("failed to prompt for save password: %w", err)
			}
			pending.ShouldSave = save
			pending.UpdateProfile = save
		}

		return buildDSNWithUserPass(profile.DSN, username, password), pending, nil
	}

	// Build DSN with credentials
	return buildDSNWithUserPass(profile.DSN, username, password), nil, nil
}

// buildDSNWithUserPass inserts username and password into a DSN
func buildDSNWithUserPass(dsn, username, password string) string {
	// Parse DSN to extract scheme, host, port, path, and query
	parts := strings.SplitN(dsn, "://", 2)
	if len(parts) != 2 {
		return dsn
	}

	scheme := parts[0]
	rest := parts[1]

	// Split into authority and path/query
	var authority, pathQuery string
	if idx := strings.Index(rest, "/"); idx != -1 {
		authority = rest[:idx]
		pathQuery = rest[idx:]
	} else {
		authority = rest
		pathQuery = ""
	}

	// Build userinfo
	userinfo := fmt.Sprintf("%s:%s", url.QueryEscape(username), url.QueryEscape(password))

	// Reconstruct DSN
	return fmt.Sprintf("%s://%s@%s%s", scheme, userinfo, authority, pathQuery)
}

func init() {
	rootCmd.AddCommand(connectCmd)
	connectCmd.Flags().String("name", "", "Name this session (tmux-like)")
	connectCmd.Flags().StringVar(&profileName, "profile", "", "Connect using a saved profile from ~/.swissql/connections.json")
	connectCmd.Flags().Bool("disconnect-current", false, "Disconnect the currently active session before connecting")
}
