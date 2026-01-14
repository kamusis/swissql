package cmd

import (
	"fmt"
	"strings"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func connectReplFlagSet() *pflag.FlagSet {
	fs := pflag.NewFlagSet("repl-connect", pflag.ContinueOnError)
	fs.ParseErrorsWhitelist.UnknownFlags = true

	fs.String("name", "", "Name this session (tmux-like)")
	fs.String("profile", "", "Connect using a saved profile from ~/.swissql/connections.json")
	fs.Bool("disconnect-current", false, "Disconnect the currently active session before connecting")

	return fs
}

func levenshteinDistance(a string, b string) int {
	a = strings.ToLower(strings.TrimSpace(a))
	b = strings.ToLower(strings.TrimSpace(b))
	if a == b {
		return 0
	}
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}

	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := 0; j <= len(b); j++ {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		ai := a[i-1]
		for j := 1; j <= len(b); j++ {
			cost := 0
			if ai != b[j-1] {
				cost = 1
			}
			del := prev[j] + 1
			ins := curr[j-1] + 1
			sub := prev[j-1] + cost
			m := del
			if ins < m {
				m = ins
			}
			if sub < m {
				m = sub
			}
			curr[j] = m
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

func suggestFlag(unknown string, fs *pflag.FlagSet) (string, bool) {
	if fs == nil {
		return "", false
	}

	unknown = strings.TrimSpace(unknown)
	best := ""
	bestDist := 1 << 30

	fs.VisitAll(func(f *pflag.Flag) {
		if f == nil {
			return
		}
		cand := "--" + f.Name
		d := levenshteinDistance(unknown, cand)
		if d < bestDist {
			bestDist = d
			best = cand
		}
		if strings.TrimSpace(f.Shorthand) != "" {
			shortCand := "-" + strings.TrimSpace(f.Shorthand)
			ds := levenshteinDistance(unknown, shortCand)
			if ds < bestDist {
				bestDist = ds
				best = shortCand
			}
		}
	})

	if best != "" && bestDist <= 2 {
		return best, true
	}
	return "", false
}

func isKnownFlag(option string, fs *pflag.FlagSet) bool {
	if fs == nil {
		return false
	}
	opt := strings.TrimSpace(option)
	if strings.HasPrefix(opt, "--") {
		name := strings.TrimPrefix(opt, "--")
		return fs.Lookup(name) != nil
	}
	if strings.HasPrefix(opt, "-") {
		sh := strings.TrimPrefix(opt, "-")
		return fs.ShorthandLookup(sh) != nil
	}
	return false
}

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

	if len(fields) >= 2 && strings.HasPrefix(strings.TrimSpace(fields[1]), "--") && !strings.EqualFold(fields[1], "--profile") {
		fs := connectReplFlagSet()
		if sugg, ok := suggestFlag(fields[1], fs); ok {
			fmt.Printf("Error: unknown option %s. Did you mean %s?\n", fields[1], sugg)
			return true, config.SessionEntry{}, ""
		}
		if isKnownFlag(fields[1], fs) {
			fmt.Printf("Error: option %s is not supported in REPL. Use CLI: swissql connect %s ...\n", fields[1], fields[1])
			return true, config.SessionEntry{}, ""
		}
		fmt.Printf("Error: unknown option %s\n", fields[1])
		return true, config.SessionEntry{}, ""
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

		sessionId := strings.TrimSpace(resp.SessionId)
		if sessionId == "" {
			fmt.Println("Error: connect succeeded but session_id is empty")
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

		newEntry, newName, err := persistConnectedSession(c.BaseURL, dbType, dsn, "", sessionId)
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

	sessionId := strings.TrimSpace(resp.SessionId)
	if sessionId == "" {
		fmt.Println("Error: connect succeeded but session_id is empty")
		return true, config.SessionEntry{}, ""
	}

	newEntry, newName, err := persistConnectedSession(c.BaseURL, dbType, dsn, "", sessionId)
	if err != nil {
		fmt.Printf("%v\n", err)
		return true, config.SessionEntry{}, ""
	}
	return true, newEntry, newName
}
