package cmd

import (
	"fmt"
	"strings"

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

	if len(fields) >= 3 && strings.EqualFold(fields[1], "--profile") {
		profileName := strings.TrimSpace(strings.Trim(fields[2], "\"'"))
		if profileName == "" {
			fmt.Println("Error: connect --profile requires a profile name")
			return true, config.SessionEntry{}, ""
		}

		if shouldRecordHistory(historyMode, input, false) {
			line.AppendHistory("connect --profile " + profileName)
		}

		profile, err := config.GetProfile(profileName)
		if err != nil {
			fmt.Printf("failed to load profile: %v\n", err)
			return true, config.SessionEntry{}, ""
		}
		if profile == nil {
			fmt.Printf("profile '%s' not found\n", profileName)
			return true, config.SessionEntry{}, ""
		}

		dbType := config.NormalizeDbType(profile.DBType)
		dsn, pending, err := buildDSNWithCredentials(profile)
		if err != nil {
			fmt.Printf("failed to build DSN: %v\n", err)
			return true, config.SessionEntry{}, ""
		}

		resp, err := connectWithClient(c, int(c.Timeout.Milliseconds()), false, dsn, dbType)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true, config.SessionEntry{}, ""
		}

		if pending != nil && pending.ShouldSave {
			if pending.UpdateProfile {
				profile.SavePassword = true
				if _, err := config.AddProfile(profileName, profile, config.ConflictOverwrite); err != nil {
					fmt.Printf("failed to update profile: %v\n", err)
					return true, config.SessionEntry{}, ""
				}
			}
			if err := config.SetCredentials(profile.ID, pending.Username, pending.Password); err != nil {
				fmt.Printf("failed to save credentials: %v\n", err)
				return true, config.SessionEntry{}, ""
			}
		}

		newEntry, newName, err := persistConnectedSession(c.BaseURL, dbType, dsn, "", resp.SessionId)
		if err != nil {
			fmt.Printf("%v\n", err)
			return true, config.SessionEntry{}, ""
		}
		return true, newEntry, newName
	}

	dsn := strings.TrimSpace(strings.Trim(trimmed[len(fields[0]):], " \t"))
	dsn = strings.Trim(dsn, "\"'")
	if dsn == "" {
		fmt.Println("Error: connect requires a DSN")
		return true, config.SessionEntry{}, ""
	}

	if shouldRecordHistory(historyMode, input, false) {
		masked := config.MaskDsn(dsn)
		line.AppendHistory("connect " + masked)
	}

	dbType := inferDbTypeFromDsn(dsn)
	resp, err := connectWithClient(c, int(c.Timeout.Milliseconds()), false, dsn, dbType)
	if err != nil {
		fmt.Printf("%v\n", err)
		return true, config.SessionEntry{}, ""
	}

	newEntry, newName, err := persistConnectedSession(c.BaseURL, dbType, dsn, "", resp.SessionId)
	if err != nil {
		fmt.Printf("%v\n", err)
		return true, config.SessionEntry{}, ""
	}
	return true, newEntry, newName
}
