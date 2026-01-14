package cmd

import (
	"fmt"
	"os"

	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/kamusis/swissql/swissql-cli/internal/dbeaver"
	"github.com/spf13/cobra"
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

// importDbeaverProjectCmd represents the import-dbeaver-project command
var importDbeaverProjectCmd = &cobra.Command{
	Use:   "import-dbeaver-project",
	Short: "Import DBeaver .dbp project connections",
	Long: `Import connection profiles from a DBeaver .dbp project file.
Profiles are saved to ~/.swissql/connections.json.

Note: Credentials are NOT imported for security reasons.
Users must enter credentials on first use with 'connect --profile'.`,
	RunE: runImportDbeaverProject,
}

func init() {
	rootCmd.AddCommand(importDbeaverProjectCmd)

	importDbeaverProjectCmd.Flags().StringVar(&dbpPath, "dbp", "", "Path to the .dbp file (required)")
	importDbeaverProjectCmd.Flags().StringVar(&connPrefix, "conn_prefix", "", "Prefix for profile names (default: empty)")
	importDbeaverProjectCmd.Flags().StringVar(&onConflict, "on_conflict", "skip", "Conflict behavior: fail, skip, overwrite (default: skip)")
	importDbeaverProjectCmd.Flags().BoolVar(&dryRun, "dry_run", false, "Show what would be created without writing files")

	importDbeaverProjectCmd.MarkFlagRequired("dbp")
}

func runImportDbeaverProject(cmd *cobra.Command, args []string) error {
	// Validate on_conflict value
	strategy := config.ConflictStrategy(onConflict)
	if strategy != config.ConflictFail && strategy != config.ConflictSkip && strategy != config.ConflictOverwrite {
		return fmt.Errorf("invalid on_conflict value: %s (must be fail, skip, or overwrite)", onConflict)
	}

	// Parse DBP file
	archive, err := dbeaver.ParseDBP(dbpPath)
	if err != nil {
		return fmt.Errorf("failed to parse dbp file: %w", err)
	}

	if archive.DataSources == nil {
		return fmt.Errorf("no data sources found in dbp file")
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
			created, err := config.AddProfile(profileName, profile, strategy)
			if err != nil {
				result.Errors = append(result.Errors, dbeaver.ImportError{
					ConnectionName: conn.Name,
					Message:        fmt.Sprintf("failed to save: %v", err),
				})
				continue
			}

			if created {
				result.Created++
			} else {
				switch strategy {
				case config.ConflictSkip:
					result.Skipped++
				case config.ConflictOverwrite:
					result.Overwritten++
				}
			}
		}
	}

	// Render output
	renderImportResult(result, dryRun, profilesToCreate)

	// Print warning about credentials
	if !dryRun {
		fmt.Fprintf(os.Stderr, "\nWarning: Credentials were NOT imported from DBeaver for security reasons.\n")
		fmt.Fprintf(os.Stderr, "Use 'swissql connect --profile <name>' to connect with a profile.\n")
	}

	return nil
}

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
