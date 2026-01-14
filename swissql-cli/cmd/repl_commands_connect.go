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

// levenshteinDistance computes the edit distance between two short strings.
// It is used for suggesting the closest CLI flag when users mistype an option.
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

// suggestFlag returns the closest matching flag name/shorthand for an unknown option.
// This is a best-effort UX improvement for REPL commands.
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

// isKnownFlag checks whether an option is a defined flag in the given FlagSet.
// It supports both long form (--flag) and shorthand (-f).
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

// connectUsingProfileName resolves a saved profile, builds a DSN with credentials,
// connects via backend, and persists the resulting session.
//
// This helper is shared by both:
// - `connect --profile <name>`
// - `connect <name>` shorthand when <name> matches an existing profile.
func connectUsingProfileName(
	line *liner.State,
	historyMode string,
	input string,
	c *client.Client,
	profileName string,
) (connected bool, entry config.SessionEntry, name string) {
	resolvedName := strings.TrimSpace(strings.Trim(profileName, "\"'"))
	if resolvedName == "" {
		fmt.Println("Error: connect --profile requires a profile name")
		return true, config.SessionEntry{}, ""
	}

	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory("connect --profile " + resolvedName)
	}

	profile, err := config.GetProfile(resolvedName)
	if err != nil {
		fmt.Printf("failed to load profile: %v\n", err)
		return true, config.SessionEntry{}, ""
	}
	if profile == nil {
		fmt.Printf("profile '%s' not found\n", resolvedName)
		return true, config.SessionEntry{}, ""
	}

	dbType := config.NormalizeDbType(profile.DBType)
	resolvedDsn, pending, err := buildDSNWithCredentialsForRepl(profile, line)
	if err != nil {
		fmt.Printf("failed to build DSN: %v\n", err)
		return true, config.SessionEntry{}, ""
	}

	resp, err := connectWithClient(c, int(c.Timeout.Milliseconds()), false, resolvedDsn, dbType)
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
			if _, err := config.AddProfile(resolvedName, profile, config.ConflictOverwrite); err != nil {
				fmt.Printf("failed to update profile: %v\n", err)
				return true, config.SessionEntry{}, ""
			}
		}
		if err := config.SetCredentials(profile.ID, pending.Username, pending.Password); err != nil {
			fmt.Printf("failed to save credentials: %v\n", err)
			return true, config.SessionEntry{}, ""
		}
	}

	newEntry, newName, err := persistConnectedSession(c.BaseURL, dbType, resolvedDsn, "", sessionId)
	if err != nil {
		fmt.Printf("%v\n", err)
		return true, config.SessionEntry{}, ""
	}
	return true, newEntry, newName
}

// buildDSNWithCredentialsForRepl builds a DSN with credentials for REPL usage.
//
// In REPL mode, stdin/terminal is managed by liner, so we must prompt via
// liner instead of reading os.Stdin directly.
func buildDSNWithCredentialsForRepl(profile *config.Profile, line *liner.State) (string, *pendingCredentialSave, error) {
	username, password, err := config.GetCredentials(profile.ID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to load credentials: %w", err)
	}

	if strings.TrimSpace(username) == "" && strings.TrimSpace(password) == "" {
		if line == nil {
			return "", nil, fmt.Errorf("failed to prompt for credentials: REPL input is not available")
		}

		u, err := line.Prompt("Username: ")
		if err != nil {
			return "", nil, fmt.Errorf("failed to prompt for username: %w", err)
		}
		p, err := line.PasswordPrompt("Password: ")
		if err != nil {
			return "", nil, fmt.Errorf("failed to prompt for password: %w", err)
		}

		username = strings.TrimSpace(u)
		password = strings.TrimSpace(p)

		pending := &pendingCredentialSave{Username: username, Password: password}
		if profile.SavePassword {
			pending.ShouldSave = true
		} else {
			resp, err := line.Prompt("Save password for future use? [y/N]: ")
			if err != nil {
				return "", nil, fmt.Errorf("failed to prompt for save password: %w", err)
			}
			save := strings.TrimSpace(strings.ToLower(resp)) == "y"
			pending.ShouldSave = save
			pending.UpdateProfile = save
		}

		return buildDSNWithUserPass(profile.DSN, username, password), pending, nil
	}

	return buildDSNWithUserPass(profile.DSN, username, password), nil, nil
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
		return connectUsingProfileName(line, historyMode, input, c, fields[2])
	}

	dsn := strings.TrimSpace(strings.Trim(trimmed[len(fields[0]):], " \t"))
	dsn = strings.Trim(dsn, "\"'")
	if dsn == "" {
		fmt.Println("Error: connect requires a DSN")
		return true, config.SessionEntry{}, ""
	}

	if !strings.Contains(dsn, "://") {
		profileName := strings.TrimSpace(dsn)
		profile, err := config.GetProfile(profileName)
		if err != nil {
			fmt.Printf("failed to load profile: %v\n", err)
			return true, config.SessionEntry{}, ""
		}
		if profile == nil {
			fmt.Println("Error: connect requires a DSN in the form '<db_type>://...'.")
			fmt.Println("Tip: use 'connect --profile <name>' for saved profiles.")
			return true, config.SessionEntry{}, ""
		}

		return connectUsingProfileName(line, historyMode, input, c, profileName)
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
