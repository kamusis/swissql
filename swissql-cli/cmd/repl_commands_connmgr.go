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

var (
	dbpPath    string
	connPrefix string
	onConflict string
	dryRun     bool
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
func runConnmgrImport(_ *replDispatchContext) (bool, bool) {
	// Validate on_conflict value
	strategy := config.ConflictStrategy(onConflict)
	if strategy != config.ConflictFail && strategy != config.ConflictSkip && strategy != config.ConflictOverwrite {
		fmt.Printf("Error: invalid on_conflict value: %s (must be fail, skip, or overwrite)\n", onConflict)
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

	// Convert connections
	result := &dbeaver.ImportResult{
		Discovered: len(archive.DataSources.Connections),
		Errors:     []dbeaver.ImportError{},
	}

	var profilesToCreate []profileInfo

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

		// Add profile (or dry run)
		// Check if profile already exists (for both dry_run and actual import)
		profiles, err := config.LoadProfiles()
		if err != nil {
			result.Errors = append(result.Errors, dbeaver.ImportError{
				ConnectionName: conn.Name,
				Message:        fmt.Sprintf("failed to load profiles: %v", err),
			})
			continue
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
			// Add or overwrite profile
			_, err = config.AddProfile(profileName, profile, strategy)
			if err != nil {
				result.Errors = append(result.Errors, dbeaver.ImportError{
					ConnectionName: conn.Name,
					Message:        fmt.Sprintf("failed to save: %v", err),
				})
				continue
			}

			// Update counters
			if exists {
				result.Overwritten++
			} else {
				result.Created++
			}
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

// runConnmgrRemove handles the connmgr remove command (placeholder)
func runConnmgrRemove(_ *replDispatchContext) (bool, bool) {
	fmt.Println("connmgr remove command not yet implemented")
	return true, false
}

// runConnmgrShow handles the connmgr show command (placeholder)
func runConnmgrShow(_ *replDispatchContext) (bool, bool) {
	fmt.Println("connmgr show command not yet implemented")
	return true, false
}

// runConnmgrUpdate handles the connmgr update command (placeholder)
func runConnmgrUpdate(_ *replDispatchContext) (bool, bool) {
	fmt.Println("connmgr update command not yet implemented")
	return true, false
}
