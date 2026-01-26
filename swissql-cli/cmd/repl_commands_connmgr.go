package cmd

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/kamusis/swissql/swissql-cli/internal/dbeaver"
)

type profileInfo struct {
	Name         string
	DBType       string
	DSN          string
	SavePassword bool
	Source       config.Source
}

// renderImportResult renders the import result
func renderImportResult(result *dbeaver.ImportResult, dryRun bool, profilesToCreate []profileInfo) {
	fmt.Println("Import completed:")
	fmt.Printf("  Discovered: %d connections\n", result.Discovered)
	fmt.Printf("  Created: %d profiles\n", result.Created)
	fmt.Printf("  Skipped: %d\n", result.Skipped)
	fmt.Printf("  Overwritten: %d\n", result.Overwritten)

	if len(result.Errors) > 0 {
		fmt.Println("\nErrors:")
		for _, err := range result.Errors {
			fmt.Printf("  - %s: %s\n", err.ConnectionName, err.Message)
		}
	}

	// Show detailed profile information in dry_run mode
	if dryRun && len(profilesToCreate) > 0 {
		fmt.Println("\nProfiles to be created:")
		for i, p := range profilesToCreate {
			fmt.Printf("\n  [%d] %s\n", i+1, p.Name)
			fmt.Printf("      db_type: %s\n", p.DBType)
			fmt.Printf("      dsn: %s\n", p.DSN)
			fmt.Printf("      save_password: %v\n", p.SavePassword)
			fmt.Printf("      source: %s (provider=%s, driver=%s)\n", p.Source.Kind, p.Source.Provider, p.Source.Driver)
		}
	}
}

// runConnmgrImport handles the connmgr import -dbp command
func runConnmgrImport(ctx *replDispatchContext) (bool, bool) {
	// Parse flags using robust helper
	flags := parseReplFlags(ctx.MetaArgs)
	dbpPath := flags["-dbp"]
	connPrefix := flags["--conn_prefix"]
	onConflictStr := flags["--on_conflict"]
	if onConflictStr == "" {
		onConflictStr = "skip" // default
	}
	dryRun := flags["--dry_run"] == "true"

	if dbpPath == "" {
		fmt.Println("Error: -dbp flag is required for import")
		return true, false
	}

	// Validate on_conflict value
	strategy := config.ConflictStrategy(onConflictStr)
	if strategy != config.ConflictFail && strategy != config.ConflictSkip && strategy != config.ConflictOverwrite {
		fmt.Printf("Error: invalid on_conflict value: %s (must be fail, skip, or overwrite)\n", onConflictStr)
		return true, false
	}

	// Parse DBP file
	archive, err := dbeaver.ParseDBP(dbpPath)
	if err != nil {
		fmt.Printf("Error: failed to parse dbp file: %v\n", err)
		return true, false
	}

	if archive.DataSources == nil {
		fmt.Println("Error: no data sources found in dbp file")
		return true, false
	}

	// Load profiles once
	profiles, err := config.LoadProfiles()
	if err != nil {
		fmt.Printf("Error: failed to load profiles: %v\n", err)
		return true, false
	}

	// Convert connections
	result := &dbeaver.ImportResult{
		Discovered: len(archive.DataSources.Connections),
		Errors:     []dbeaver.ImportError{},
	}

	var profilesToCreate []profileInfo
	modified := false

	for connID, conn := range archive.DataSources.Connections {
		profile, err := dbeaver.ConvertConnection(&conn, connPrefix)
		if err != nil {
			result.Errors = append(result.Errors, dbeaver.ImportError{
				ConnectionName: conn.Name,
				Message:        fmt.Sprintf("failed to convert: %v", err),
			})
			continue
		}

		// Set connection ID in source
		profile.Source.ConnectionID = connID

		// Sanitize profile name
		profileName := dbeaver.SanitizeProfileName(conn.Name)
		if connPrefix != "" {
			profileName = fmt.Sprintf("%s_%s", connPrefix, profileName)
		}

		_, exists := profiles.Connections[profileName]

		// Handle conflict strategies
		if exists {
			switch strategy {
			case config.ConflictFail:
				result.Errors = append(result.Errors, dbeaver.ImportError{
					ConnectionName: conn.Name,
					Message:        fmt.Sprintf("profile '%s' already exists (conflict strategy: fail)", profileName),
				})
				continue
			case config.ConflictSkip:
				result.Skipped++
				continue
			case config.ConflictOverwrite:
				// Proceed to overwrite
			}
		}

		if dryRun {
			result.Created++
			profilesToCreate = append(profilesToCreate, profileInfo{
				Name:         profileName,
				DBType:       profile.DBType,
				DSN:          profile.DSN,
				SavePassword: profile.SavePassword,
				Source:       profile.Source,
			})
		} else {
			// Update in-memory map
			profiles.Connections[profileName] = *profile
			modified = true

			// Update counters
			if exists {
				result.Overwritten++
			} else {
				result.Created++
			}
		}
	}

	// Save changed profiles once
	if !dryRun && modified {
		if err := config.SaveProfiles(profiles); err != nil {
			fmt.Printf("Error: failed to save connections.json: %v\n", err)
			return true, false
		}
	}

	// Render output
	renderImportResult(result, dryRun, profilesToCreate)

	// Print warning about credentials
	if !dryRun {
		fmt.Fprintf(os.Stderr, "\nNote: Credentials were NOT imported from DBeaver for security reasons.\n")
		fmt.Fprintf(os.Stderr, "Use 'swissql connect --profile <name>' to connect with a profile.\n")
	}

	return true, false
}

// runConnmgrList handles the connmgr list command
func runConnmgrList(ctx *replDispatchContext) (bool, bool) {
	// Parse filters from command args
	filters, err := parseConnmgrFilters(ctx.MetaArgs)
	if err != nil {
		fmt.Printf("%v\n", err)
		return true, false
	}
	if err := validateConnmgrFilterKeys(filters); err != nil {
		fmt.Printf("%v\n", err)
		return true, false
	}

	start := time.Now()
	profiles, err := config.LoadProfiles()
	if err != nil {
		fmt.Printf("%v\n", err)
		return true, false
	}

	rows := make([]map[string]interface{}, 0, len(profiles.Connections))
	for name, p := range profiles.Connections {
		if !matchConnmgrFilters(name, p, filters) {
			continue
		}

		rows = append(rows, map[string]interface{}{
			"name":          name,
			"db_type":       p.DBType,
			"dsn":           config.MaskDsn(p.DSN),
			"save_password": p.SavePassword,
		})
	}

	// Sort by db_type, then name
	sort.Slice(rows, func(i, j int) bool {
		dbTypeI := rows[i]["db_type"].(string)
		dbTypeJ := rows[j]["db_type"].(string)
		if dbTypeI != dbTypeJ {
			return dbTypeI < dbTypeJ
		}
		nameI := rows[i]["name"].(string)
		nameJ := rows[j]["name"].(string)
		return nameI < nameJ
	})

	resp := &client.ExecuteResponse{
		Type: "tabular",
		Data: client.DataContent{
			Columns: []client.ColumnDefinition{
				{Name: "name", Type: "string"},
				{Name: "db_type", Type: "string"},
				{Name: "dsn", Type: "string"},
				{Name: "save_password", Type: "bool"},
			},
			Rows: rows,
		},
		Metadata: client.ResponseMetadata{
			DurationMs:   int(time.Since(start).Milliseconds()),
			RowsAffected: len(rows),
			Truncated:    false,
		},
	}

	renderResponse(ctx.Cmd, resp)
	return true, false
}

// validateConnmgrFilterKeys validates filter keys
func validateConnmgrFilterKeys(filters map[string][]string) error {
	if len(filters) == 0 {
		return nil
	}

	supported := map[string]struct{}{
		"name":          {},
		"db_type":       {},
		"dsn":           {},
		"save_password": {},
	}

	unknown := make([]string, 0)
	for k := range filters {
		if _, ok := supported[k]; !ok {
			unknown = append(unknown, k)
		}
	}
	if len(unknown) == 0 {
		return nil
	}

	return fmt.Errorf("unknown filter key(s): %s", strings.Join(unknown, ", "))
}

// parseConnmgrFilters parses one or more --filter key=value tokens from command args
func parseConnmgrFilters(args []string) (map[string][]string, error) {
	filters := map[string][]string{}
	for i := 0; i < len(args); i++ {
		if args[i] != "--filter" {
			continue
		}
		if i+1 >= len(args) {
			return nil, fmt.Errorf("--filter requires key=value")
		}
		kv := strings.TrimSpace(strings.Trim(args[i+1], "\"'"))
		i++

		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid filter %q (expected key=value)", kv)
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])
		if key == "" || val == "" {
			return nil, fmt.Errorf("invalid filter %q (expected key=value)", kv)
		}
		filters[key] = append(filters[key], val)
	}
	return filters, nil
}

// matchConnmgrFilters applies composite filtering (AND across keys, OR across repeated values for a key)
func matchConnmgrFilters(name string, p config.Profile, filters map[string][]string) bool {
	if len(filters) == 0 {
		return true
	}

	getField := func(key string) string {
		switch key {
		case "name":
			return name
		case "db_type":
			return p.DBType
		case "dsn":
			return p.DSN
		case "save_password":
			return fmt.Sprintf("%v", p.SavePassword)
		default:
			return ""
		}
	}

	for key, vals := range filters {
		field := getField(key)

		matchedOne := false
		for _, v := range vals {
			if key == "dsn" || key == "url" {
				if strings.Contains(strings.ToLower(field), strings.ToLower(v)) {
					matchedOne = true
					break
				}
				continue
			}
			if strings.EqualFold(strings.TrimSpace(field), strings.TrimSpace(v)) {
				matchedOne = true
				break
			}
		}
		if !matchedOne {
			return false
		}
	}
	return true
}

// runConnmgrRemove handles the connmgr remove command
func runConnmgrRemove(ctx *replDispatchContext) (bool, bool) {
	args := ctx.MetaArgs
	if len(args) == 0 {
		fmt.Println("Error: profile name required")
		fmt.Println("Usage: connmgr remove <profile-name> [--db_type <name>] [--like] [--force]")
		fmt.Println("       connmgr remove --db_type <name> [--like] [--force]")
		return true, false
	}

	var profileName string
	var flagArgs []string

	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		profileName = args[0]
		flagArgs = args[1:]
	} else {
		profileName = ""
		flagArgs = args
	}

	// Parse flags using robust helper
	flags := parseReplFlags(flagArgs)
	dbType := flags["--db_type"]
	useLike := flags["--like"] == "true"
	force := flags["--force"] == "true"

	// Validate: profile name is required unless --db_type is provided
	if strings.TrimSpace(profileName) == "" && dbType == "" {
		fmt.Println("Error: profile name required (or use --db_type to filter by database type)")
		fmt.Println("Usage: connmgr remove <profile-name> [--db_type <name>] [--like] [--force]")
		fmt.Println("       connmgr remove --db_type <name> [--like] [--force]")
		return true, false
	}

	// Load profiles
	profiles, err := config.LoadProfiles()
	if err != nil {
		fmt.Printf("Error: failed to load profiles: %v\n", err)
		return true, false
	}

	// Find matching profiles
	var matchedProfiles []string
	for name, profile := range profiles.Connections {
		nameMatch := false
		dbTypeMatch := false

		// Check name match (only if not using --db_type filter)
		if dbType == "" {
			if useLike {
				// Only match if profileName is not empty
				if profileName != "" {
					nameMatch = strings.Contains(strings.ToLower(name), strings.ToLower(profileName))
				}
			} else {
				nameMatch = strings.EqualFold(name, profileName)
			}
		} else {
			// When --db_type is provided, profile name is optional or used with --like
			if useLike {
				// Only match if profileName is not empty
				if profileName != "" {
					nameMatch = strings.Contains(strings.ToLower(name), strings.ToLower(profileName))
				}
				// If profileName is empty, don't match by name (only match by db_type)
			} else {
				// If not using --like, match any profile name when --db_type is provided
				nameMatch = true
			}
		}

		// Check db_type match if --db_type is provided
		if dbType != "" {
			if useLike {
				dbTypeMatch = strings.Contains(strings.ToLower(profile.DBType), strings.ToLower(dbType))
			} else {
				dbTypeMatch = strings.EqualFold(profile.DBType, dbType)
			}
		}

		// Include profile if it matches name and (no db_type filter or db_type matches)
		if nameMatch && (dbType == "" || dbTypeMatch) {
			matchedProfiles = append(matchedProfiles, name)
		}
	}

	if len(matchedProfiles) == 0 {
		fmt.Printf("Error: no profiles found matching criteria\n")
		return true, false
	}

	// If multiple profiles matched and not using --force, list and confirm
	if len(matchedProfiles) > 1 && !force {
		fmt.Printf("Found %d matching profiles:\n", len(matchedProfiles))
		for i, name := range matchedProfiles {
			profile := profiles.Connections[name]
			fmt.Printf("  %d. %s (db_type: %s)\n", i+1, name, profile.DBType)
		}

		response, err := ctx.Line.Prompt("Remove all these profiles? [y/N]: ")
		if err != nil || !strings.EqualFold(strings.TrimSpace(response), "y") {
			fmt.Println("Operation cancelled")
			return true, false
		}
	}

	// Load credentials once for batch deletion
	credentials, err := config.LoadCredentials()
	if err != nil {
		fmt.Printf("Warning: failed to load credentials: %v\n", err)
		credentials = nil
	}

	// Remove each matched profile
	for _, name := range matchedProfiles {
		profile := profiles.Connections[name]

		// Prompt for confirmation for single profile unless --force flag
		if len(matchedProfiles) == 1 && !force {
			response, err := ctx.Line.Prompt(fmt.Sprintf("Are you sure you want to remove profile '%s'? [y/N]: ", name))
			if err != nil || !strings.EqualFold(strings.TrimSpace(response), "y") {
				fmt.Println("Operation cancelled")
				return true, false
			}
		}

		// Cascade delete: Remove credentials using profile.ID
		if credentials != nil && profile.ID != "" {
			delete(credentials.Credentials, profile.ID)
		}

		// Remove profile from connections.json
		delete(profiles.Connections, name)
	}

	// Save updated profiles
	if err := config.SaveProfiles(profiles); err != nil {
		fmt.Printf("Error: failed to save connections.json: %v\n", err)
		return true, false
	}

	// Save updated credentials once (if any were modified)
	if credentials != nil {
		if err := config.SaveCredentials(credentials); err != nil {
			fmt.Printf("Warning: failed to save credentials: %v\n", err)
		}
	}

	if len(matchedProfiles) == 1 {
		fmt.Printf("Profile '%s' removed successfully\n", matchedProfiles[0])
	} else {
		fmt.Printf("Removed %d profiles successfully\n", len(matchedProfiles))
	}
	return true, false
}

// findProfileByName is a helper that looks up a profile by name and returns it along with the profiles registry and any error.
func findProfileByName(profileName string) (*config.Profile, *config.Profiles, error) {
	if strings.TrimSpace(profileName) == "" {
		return nil, nil, fmt.Errorf("profile name cannot be empty")
	}

	profiles, err := config.LoadProfiles()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load profiles: %w", err)
	}

	profile, exists := profiles.Connections[profileName]
	if !exists {
		return nil, nil, fmt.Errorf("profile '%s' not found", profileName)
	}

	return &profile, profiles, nil
}

// runConnmgrShow handles the connmgr show command
func runConnmgrShow(ctx *replDispatchContext) (bool, bool) {
	args := ctx.MetaArgs
	if len(args) < 1 {
		fmt.Println("Error: profile name required")
		fmt.Println("Usage: connmgr show <profile-name>")
		return true, false
	}

	profile, _, err := findProfileByName(args[0])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return true, false
	}

	// Display profile information
	fmt.Printf("Profile: %s\n", args[0])
	fmt.Printf("Database Type: %s\n", profile.DBType)
	fmt.Printf("DSN: %s\n", config.MaskDsn(profile.DSN))
	fmt.Printf("JDBC URL: %s\n", profile.URL)
	fmt.Printf("Save Password: %v\n", profile.SavePassword)

	// Display source information if available
	if profile.Source.Kind != "" {
		fmt.Println("Source:")
		fmt.Printf("  Kind: %s\n", profile.Source.Kind)
		if profile.Source.Provider != "" {
			fmt.Printf("  Provider: %s\n", profile.Source.Provider)
		}
		if profile.Source.Driver != "" {
			fmt.Printf("  Driver: %s\n", profile.Source.Driver)
		}
		if profile.Source.ConnectionID != "" {
			fmt.Printf("  Connection ID: %s\n", profile.Source.ConnectionID)
		}
	}

	return true, false
}

// parseReplFlags parses multiple --flag=value or --flag value tokens from MetaArgs.
// Boolean flags (--force, --like, --dry_run) are always set to "true" and don't consume the next argument.
// Surrounding quotes are stripped from flag values.
func parseReplFlags(args []string) map[string]string {
	// Known boolean flags that should not consume subsequent arguments
	booleanFlags := map[string]bool{
		"--force":   true,
		"--like":    true,
		"--dry_run": true,
	}

	flags := make(map[string]string)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			continue
		}

		if strings.Contains(arg, "=") {
			parts := strings.SplitN(arg, "=", 2)
			flags[parts[0]] = stripQuotesUtil(parts[1])
		} else if booleanFlags[arg] {
			// Boolean flag: always set to "true", never consume next arg
			flags[arg] = "true"
		} else if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			flags[arg] = stripQuotesUtil(args[i+1])
			i++
		} else {
			// Flag without value and not in booleanFlags list - treat as boolean
			flags[arg] = "true"
		}
	}
	return flags
}

// runConnmgrUpdate handles the connmgr update command
func runConnmgrUpdate(ctx *replDispatchContext) (bool, bool) {
	args := ctx.MetaArgs
	if len(args) < 1 {
		fmt.Println("Error: profile name required")
		fmt.Println("Usage: connmgr update <profile-name> [--new-name <name>] [--dsn <dsn>] [--db-type <type>]")
		return true, false
	}

	originalName := args[0]
	profile, profiles, err := findProfileByName(originalName)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return true, false
	}
	profileName := originalName

	// Parse flags using robust helper
	flags := parseReplFlags(args[1:])
	newName := flags["--new-name"]
	newDSN := flags["--dsn"]
	newDbType := flags["--db-type"]

	// Validate at least one update parameter is provided
	if newName == "" && newDSN == "" && newDbType == "" {
		fmt.Println("Error: at least one update parameter required (--new-name, --dsn, or --db-type)")
		return true, false
	}

	// Track changes for summary
	var changes []string

	// Validate and apply --new-name
	if newName != "" {
		if strings.TrimSpace(newName) == "" {
			fmt.Println("Error: new name cannot be empty")
			return true, false
		}
		// Sanitize profile name (consistent with import command)
		sanitizedName := dbeaver.SanitizeProfileName(newName)
		if sanitizedName != profileName {
			if _, exists := profiles.Connections[sanitizedName]; exists {
				fmt.Printf("Error: profile '%s' already exists\n", sanitizedName)
				return true, false
			}
			// Remove old entry and add new one
			delete(profiles.Connections, profileName)
			profiles.Connections[sanitizedName] = *profile
			profileName = sanitizedName
			changes = append(changes, fmt.Sprintf("Name: %s", sanitizedName))
		}
	}

	// Validate and apply --dsn
	if newDSN != "" {
		if strings.TrimSpace(newDSN) == "" {
			fmt.Println("Error: DSN cannot be empty")
			return true, false
		}
		// Validate DSN format
		if err := config.ValidateDSN(newDSN); err != nil {
			fmt.Printf("Error: invalid DSN format: %v\n", err)
			fmt.Println("\nValid DSN examples:")
			fmt.Println("  - oracle://host:1521/service")
			fmt.Println("  - oracle://alias?TNS_ADMIN=/path/to/wallet")
			fmt.Println("  - postgresql://localhost:5432/database")
			fmt.Println("  - postgres://127.0.0.1:5432/postgres")
			return true, false
		}
		p := profiles.Connections[profileName]
		p.DSN = newDSN
		// Generate JDBC URL from DSN to keep them in sync
		p.URL = config.GenerateJDBCURL(newDSN)
		profiles.Connections[profileName] = p
		changes = append(changes, "DSN: updated")
	}

	// Validate and apply --db-type
	if newDbType != "" {
		normalizedDbType := config.NormalizeDbType(newDbType)
		if normalizedDbType != profiles.Connections[profileName].DBType {
			p := profiles.Connections[profileName]
			p.DBType = normalizedDbType
			profiles.Connections[profileName] = p
			changes = append(changes, fmt.Sprintf("Database Type: %s", normalizedDbType))
		}
	}

	// Save updated profiles
	if err := config.SaveProfiles(profiles); err != nil {
		fmt.Printf("Error: failed to save connections.json: %v\n", err)
		return true, false
	}

	// Display success message with summary
	fmt.Printf("Profile '%s' updated successfully:\n", profileName)
	for _, change := range changes {
		fmt.Printf("  - %s\n", change)
	}

	return true, false
}

// runConnmgrClearCredential handles the connmgr clear-credential command
func runConnmgrClearCredential(ctx *replDispatchContext) (bool, bool) {
	args := ctx.MetaArgs
	if len(args) < 1 {
		fmt.Println("Error: profile name required")
		fmt.Println("Usage: connmgr clear-credential <profile-name>")
		return true, false
	}

	profile, _, err := findProfileByName(args[0])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return true, false
	}
	profileName := args[0]

	// Load credentials
	credentials, err := config.LoadCredentials()
	if err != nil {
		fmt.Printf("Error: failed to load credentials: %v\n", err)
		return true, false
	}

	// Check if credentials exist for this profile ID
	if profile.ID == "" {
		fmt.Printf("No credentials found for profile '%s'\n", profileName)
		return true, false
	}

	if _, exists := credentials.Credentials[profile.ID]; !exists {
		fmt.Printf("No credentials found for profile '%s'\n", profileName)
		return true, false
	}

	// Prompt for confirmation
	response, err := ctx.Line.Prompt(fmt.Sprintf("Are you sure you want to clear credentials for profile '%s'? [y/N]: ", profileName))
	if err != nil {
		fmt.Printf("Error: failed to read input: %v\n", err)
		return true, false
	}
	if !strings.EqualFold(strings.TrimSpace(response), "y") {
		fmt.Println("Operation cancelled")
		return true, false
	}

	// Remove credentials
	delete(credentials.Credentials, profile.ID)

	// Save updated credentials
	if err := config.SaveCredentials(credentials); err != nil {
		fmt.Printf("Error: failed to save credentials.json: %v\n", err)
		return true, false
	}

	fmt.Printf("Credentials cleared for profile '%s'\n", profileName)
	return true, false
}
