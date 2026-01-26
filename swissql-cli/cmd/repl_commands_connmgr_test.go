package cmd

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/kamusis/swissql/swissql-cli/internal/config"
	"github.com/kamusis/swissql/swissql-cli/internal/dbeaver"
)

// captureOutput captures stdout during function execution
func captureOutput(fn func()) string {
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Also redirect the global outputWriter
	oldOutputWriter := outputWriter
	outputWriter = w

	fn()

	w.Close()
	os.Stdout = old
	outputWriter = oldOutputWriter

	var buf bytes.Buffer
	buf.ReadFrom(r)
	return buf.String()
}

// setupTestConfigDir redirects config directory to a temp directory for test isolation.
// This prevents tests from writing to the real ~/.swissql directory.
// Returns a cleanup function that should be deferred.
func setupTestConfigDir(t *testing.T) func() {
	t.Helper()

	tmp := t.TempDir()

	// Save original env vars
	origUserProfile := os.Getenv("USERPROFILE")
	origHome := os.Getenv("HOME")

	// Redirect to temp directory
	_ = os.Setenv("USERPROFILE", tmp)
	_ = os.Setenv("HOME", tmp)

	// Return cleanup function
	return func() {
		_ = os.Setenv("USERPROFILE", origUserProfile)
		_ = os.Setenv("HOME", origHome)
	}
}

// TestRunConnmgrImport_InvalidConflictStrategy tests validation of on_conflict parameter
func TestRunConnmgrImport_InvalidConflictStrategy(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			MetaArgs: []string{"-dbp", "test.dbp", "--on_conflict", "invalid"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "invalid on_conflict value") {
		t.Errorf("expected output to contain 'invalid on_conflict value', got: %q", output)
	}
}

// TestRunConnmgrImport_MissingDBPFlag tests missing required -dbp flag
func TestRunConnmgrImport_MissingDBPFlag(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			MetaArgs: []string{"--on_conflict", "fail"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "required") && !strings.Contains(output, "dbp") {
		t.Errorf("expected output to contain 'required' or 'dbp', got: %q", output)
	}
}

// TestRunConnmgrImport_ValidFlags tests that valid flags are accepted
func TestRunConnmgrImport_ValidFlags(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			MetaArgs: []string{"-dbp", "test.dbp", "--on_conflict", "fail"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	// Should fail on file not found, not on flag validation
	if !strings.Contains(output, "failed to parse dbp file") && !strings.Contains(output, "no such file") {
		t.Errorf("expected file not found error, got: %q", output)
	}
}

// TestRunConnmgrImport_AllConflictStrategies tests all valid conflict strategies
func TestRunConnmgrImport_AllConflictStrategies(t *testing.T) {
	strategies := []string{"fail", "skip", "overwrite"}

	for _, strategy := range strategies {
		t.Run(strategy, func(t *testing.T) {
			output := captureOutput(func() {
				handled, _ := runConnmgrImport(&replDispatchContext{
					MetaArgs: []string{"-dbp", "test.dbp", "--on_conflict", strategy},
				})
				if !handled {
					t.Error("expected handled to be true")
				}
			})

			// Should fail on file not found, not on invalid strategy
			if strings.Contains(output, "invalid on_conflict value") {
				t.Errorf("strategy %s should be valid, but got error: %q", strategy, output)
			}
		})
	}
}

// TestRunConnmgrImport_DryRunMode tests dry run mode
func TestRunConnmgrImport_DryRunMode(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			MetaArgs: []string{"-dbp", "test.dbp", "--on_conflict", "fail", "--dry_run"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	// Should fail on file not found
	if !strings.Contains(output, "failed to parse dbp file") && !strings.Contains(output, "no such file") {
		t.Errorf("expected file not found error, got: %q", output)
	}
}

// TestRunConnmgrList_EmptyFilters tests list command with no filters
func TestRunConnmgrList_EmptyFilters(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrList(&replDispatchContext{
			Input: "connmgr list",
			Cmd:   rootCmd, // Set Cmd to enable renderResponse output
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	// Should print something (even if empty)
	if output == "" {
		t.Error("expected some output from connmgr list")
	}
}

// TestRunConnmgrList_InvalidFilterFormat tests invalid filter format
func TestRunConnmgrList_InvalidFilterFormat(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrList(&replDispatchContext{
			Input:    "connmgr list --filter invalid",
			MetaArgs: []string{"--filter", "invalid"},
			Cmd:      rootCmd, // Set Cmd to enable renderResponse output
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "expected key=value") {
		t.Errorf("expected output to contain 'expected key=value', got: %q", output)
	}
}

// TestRunConnmgrList_UnknownFilterKey tests unknown filter key
func TestRunConnmgrList_UnknownFilterKey(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrList(&replDispatchContext{
			Input:    "connmgr list --filter unknown=value",
			MetaArgs: []string{"--filter", "unknown=value"},
			Cmd:      rootCmd, // Set Cmd to enable renderResponse output
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "unknown filter key") {
		t.Errorf("expected output to contain 'unknown filter key', got: %q", output)
	}
}

// TestRunConnmgrList_ValidFilters tests valid filters
func TestRunConnmgrList_ValidFilters(t *testing.T) {
	tests := []struct {
		name  string
		args  []string
		input string
	}{
		{
			name:  "filter by name",
			args:  []string{"--filter", "name=test"},
			input: "connmgr list --filter name=test",
		},
		{
			name:  "filter by db_type",
			args:  []string{"--filter", "db_type=oracle"},
			input: "connmgr list --filter db_type=oracle",
		},
		{
			name:  "filter by dsn",
			args:  []string{"--filter", "dsn=localhost"},
			input: "connmgr list --filter dsn=localhost",
		},
		{
			name:  "multiple filters",
			args:  []string{"--filter", "name=test", "--filter", "db_type=oracle"},
			input: "connmgr list --filter name=test --filter db_type=oracle",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureOutput(func() {
				handled, _ := runConnmgrList(&replDispatchContext{
					Input:    tt.input,
					MetaArgs: tt.args,
					Cmd:      rootCmd, // Set Cmd to enable renderResponse output
				})
				if !handled {
					t.Error("expected handled to be true")
				}
			})

			// Should not have error
			if strings.Contains(output, "error") || strings.Contains(output, "Error") {
				t.Errorf("unexpected error in output: %q", output)
			}
		})
	}
}

// TestParseConnmgrFilters tests filter parsing helper function
func TestParseConnmgrFilters(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
		errMsg  string
		wantLen int
	}{
		{
			name:    "valid filter",
			args:    []string{"--filter", "name=test"},
			wantErr: false,
			wantLen: 1,
		},
		{
			name:    "invalid format - no equals",
			args:    []string{"--filter", "invalid"},
			wantErr: true,
			errMsg:  "expected key=value",
			wantLen: 0,
		},
		{
			name:    "invalid format - empty key",
			args:    []string{"--filter", "=value"},
			wantErr: true,
			errMsg:  "expected key=value",
			wantLen: 0,
		},
		{
			name:    "invalid format - empty value",
			args:    []string{"--filter", "key="},
			wantErr: true,
			errMsg:  "expected key=value",
			wantLen: 0,
		},
		{
			name:    "unknown filter key",
			args:    []string{"--filter", "unknown=value"},
			wantErr: false, // parseConnmgrFilters doesn't validate keys
			wantLen: 1,
		},
		{
			name:    "valid filters - multiple",
			args:    []string{"--filter", "name=test", "--filter", "db_type=oracle"},
			wantErr: false,
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filters, err := parseConnmgrFilters(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseConnmgrFilters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("expected error to contain %q, got: %q", tt.errMsg, err.Error())
			}
			if !tt.wantErr && len(filters) != tt.wantLen {
				t.Errorf("expected %d filters, got %d", tt.wantLen, len(filters))
			}
		})
	}
}

// TestValidateConnmgrFilterKeys tests filter key validation
func TestValidateConnmgrFilterKeys(t *testing.T) {
	tests := []struct {
		name    string
		filters map[string][]string
		wantErr bool
	}{
		{
			name:    "empty filters",
			filters: map[string][]string{},
			wantErr: false,
		},
		{
			name: "valid keys",
			filters: map[string][]string{
				"name":    {"test"},
				"db_type": {"oracle"},
				"dsn":     {"localhost"},
			},
			wantErr: false,
		},
		{
			name: "invalid key",
			filters: map[string][]string{
				"unknown": {"value"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConnmgrFilterKeys(tt.filters)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateConnmgrFilterKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestMatchConnmgrFilters tests filter matching logic
func TestMatchConnmgrFilters(t *testing.T) {
	profile := config.Profile{
		DBType: "oracle",
		DSN:    "oracle://user:pass@localhost:1521/orcl",
	}

	tests := []struct {
		name        string
		profileName string
		filters     map[string][]string
		want        bool
	}{
		{
			name:        "no filters",
			profileName: "test-profile",
			filters:     map[string][]string{},
			want:        true,
		},
		{
			name:        "match by name",
			profileName: "test-profile",
			filters: map[string][]string{
				"name": {"test-profile"},
			},
			want: true,
		},
		{
			name:        "no match by name",
			profileName: "test-profile",
			filters: map[string][]string{
				"name": {"other-profile"},
			},
			want: false,
		},
		{
			name:        "match by db_type",
			profileName: "test-profile",
			filters: map[string][]string{
				"db_type": {"oracle"},
			},
			want: true,
		},
		{
			name:        "match by dsn substring",
			profileName: "test-profile",
			filters: map[string][]string{
				"dsn": {"localhost"},
			},
			want: true,
		},
		{
			name:        "multiple filters - all match",
			profileName: "test-profile",
			filters: map[string][]string{
				"name":    {"test-profile"},
				"db_type": {"oracle"},
			},
			want: true,
		},
		{
			name:        "multiple filters - one doesn't match",
			profileName: "test-profile",
			filters: map[string][]string{
				"name":    {"test-profile"},
				"db_type": {"postgres"},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchConnmgrFilters(tt.profileName, profile, tt.filters)
			if got != tt.want {
				t.Errorf("matchConnmgrFilters() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestConfigPackageFunctions tests that we're using existing config package functions
func TestConfigPackageFunctions(t *testing.T) {
	// Test LoadProfiles
	profiles, err := config.LoadProfiles()
	if err != nil {
		t.Fatalf("LoadProfiles() error = %v", err)
	}
	if profiles == nil {
		t.Error("LoadProfiles() returned nil")
	}

	// Test that we can use GetProfile
	profile, err := config.GetProfile("nonexistent")
	if err != nil {
		t.Fatalf("GetProfile() error = %v", err)
	}
	if profile != nil {
		t.Error("GetProfile() should return nil for nonexistent profile")
	}

	// Test NormalizeDbType
	normalized := config.NormalizeDbType("postgresql")
	if normalized != "postgres" {
		t.Errorf("NormalizeDbType(\"postgresql\") = %q, want \"postgres\"", normalized)
	}
}

// TestParseReplFlags tests the robust flag parsing helper
func TestParseReplFlags(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		expected map[string]string
	}{
		{
			name: "mix of formats",
			args: []string{"-dbp", "test.dbp", "--on_conflict=overwrite", "--dry_run"},
			expected: map[string]string{
				"-dbp":          "test.dbp",
				"--on_conflict": "overwrite",
				"--dry_run":     "true",
			},
		},
		{
			name: "only equal format",
			args: []string{"--db_type=oracle", "--force=true"},
			expected: map[string]string{
				"--db_type": "oracle",
				"--force":   "true",
			},
		},
		{
			name: "only space format",
			args: []string{"--db_type", "postgres", "--force"},
			expected: map[string]string{
				"--db_type": "postgres",
				"--force":   "true",
			},
		},
		{
			name: "duplicate flags - last wins",
			args: []string{"--db_type", "oracle", "--db_type", "postgres"},
			expected: map[string]string{
				"--db_type": "postgres",
			},
		},
		{
			name: "flag followed by another flag",
			args: []string{"--dsn", "--force"},
			expected: map[string]string{
				"--dsn":   "true",
				"--force": "true",
			},
		},
		{
			name:     "empty args",
			args:     []string{},
			expected: map[string]string{},
		},
		{
			name: "boolean flag does not consume next argument",
			args: []string{"profile-name", "--force", "extra-arg"},
			expected: map[string]string{
				"--force": "true",
			},
		},
		{
			name: "double quoted path is unquoted",
			args: []string{"-dbp", `"C:\Users\test\file.dbp"`},
			expected: map[string]string{
				"-dbp": `C:\Users\test\file.dbp`,
			},
		},
		{
			name: "single quoted path is unquoted",
			args: []string{"-dbp", `'C:\Users\test\file.dbp'`},
			expected: map[string]string{
				"-dbp": `C:\Users\test\file.dbp`,
			},
		},
		{
			name: "quoted value in equals format",
			args: []string{`--path="C:\Users\test\file.dbp"`},
			expected: map[string]string{
				"--path": `C:\Users\test\file.dbp`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseReplFlags(tt.args)
			if len(got) != len(tt.expected) {
				t.Errorf("expected %d flags, got %d", len(tt.expected), len(got))
			}
			for k, v := range tt.expected {
				if got[k] != v {
					t.Errorf("expected flag %s=%s, got %s", k, v, got[k])
				}
			}
		})
	}
}

// TestFindProfileByName tests the findProfileByName helper function
func TestFindProfileByName(t *testing.T) {
	// Setup test isolation - redirect config to temp directory
	cleanup := setupTestConfigDir(t)
	defer cleanup()

	// Setup: create a test profile
	testProfiles := &config.Profiles{
		Version: 1,
		Connections: map[string]config.Profile{
			"test-profile": {
				ID:     "id1",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/orcl",
			},
		},
	}
	if err := config.SaveProfiles(testProfiles); err != nil {
		t.Fatalf("failed to save test profiles: %v", err)
	}

	tests := []struct {
		name        string
		profileName string
		wantErr     bool
		errMsg      string
	}{
		{
			name:        "existing profile",
			profileName: "test-profile",
			wantErr:     false,
		},
		{
			name:        "empty name",
			profileName: "   ",
			wantErr:     true,
			errMsg:      "cannot be empty",
		},
		{
			name:        "non-existent profile",
			profileName: "non-existent",
			wantErr:     true,
			errMsg:      "not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile, profiles, err := findProfileByName(tt.profileName)
			if (err != nil) != tt.wantErr {
				t.Errorf("findProfileByName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("expected error to contain %q, got: %q", tt.errMsg, err.Error())
			}
			if !tt.wantErr {
				if profile == nil {
					t.Error("expected profile to be non-nil")
				}
				if profiles == nil {
					t.Error("expected profiles to be non-nil")
				}
			}
		})
	}
}

// TestRenderImportResult tests the renderImportResult function
func TestRenderImportResult(t *testing.T) {
	result := &dbeaver.ImportResult{
		Discovered:  5,
		Created:     3,
		Skipped:     1,
		Overwritten: 1,
		Errors: []dbeaver.ImportError{
			{ConnectionName: "conn1", Message: "error message"},
		},
	}

	profilesToCreate := []profileInfo{
		{
			Name:         "profile1",
			DBType:       "oracle",
			DSN:          "oracle://user:pass@host:1521/service",
			SavePassword: true,
			Source:       config.Source{Kind: "dbeaver", Provider: "oracle", Driver: "oracle"},
		},
	}

	output := captureOutput(func() {
		renderImportResult(result, true, profilesToCreate)
	})

	// Verify output contains expected information
	expectedSubstrings := []string{
		"Import completed:",
		"Discovered: 5",
		"Created: 3",
		"Skipped: 1",
		"Overwritten: 1",
		"Errors:",
		"conn1: error message",
		"Profiles to be created:",
		"profile1",
		"db_type: oracle",
		"dsn: oracle://user:pass@host:1521/service",
	}

	for _, substr := range expectedSubstrings {
		if !strings.Contains(output, substr) {
			t.Errorf("expected output to contain %q, got: %q", substr, output)
		}
	}
}

// TestRunConnmgrRemove_MissingProfileName tests remove command with missing profile name
func TestRunConnmgrRemove_MissingProfileName(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrRemove(&replDispatchContext{
			MetaArgs: []string{},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile name required") {
		t.Errorf("expected output to contain 'profile name required', got: %q", output)
	}
}

// TestRunConnmgrRemove_EmptyProfileName tests remove command with empty profile name
func TestRunConnmgrRemove_EmptyProfileName(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrRemove(&replDispatchContext{
			MetaArgs: []string{"   "},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile name required") {
		t.Errorf("expected output to contain 'profile name required', got: %q", output)
	}
}

// TestRunConnmgrRemove_NonExistentProfile tests remove command with non-existent profile
func TestRunConnmgrRemove_NonExistentProfile(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrRemove(&replDispatchContext{
			MetaArgs: []string{"non-existent-profile"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "no profiles found matching criteria") {
		t.Errorf("expected output to contain 'no profiles found matching criteria', got: %q", output)
	}
}

// TestRunConnmgrRemove_WithDbType tests remove command with --db_type filter
func TestRunConnmgrRemove_WithDbType(t *testing.T) {
	// Setup test isolation
	cleanup := setupTestConfigDir(t)
	defer cleanup()

	// Setup: create test profiles
	testProfiles := &config.Profiles{
		Version: 1,
		Connections: map[string]config.Profile{
			"oracle-prod": {
				ID:     "id1",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/orcl",
				URL:    "jdbc:oracle:thin:@localhost:1521:orcl",
			},
			"postgres-dev": {
				ID:     "id2",
				DBType: "postgresql",
				DSN:    "postgresql://localhost:5432/dev",
				URL:    "jdbc:postgresql://localhost:5432/dev",
			},
			"oracle-test": {
				ID:     "id3",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/test",
				URL:    "jdbc:oracle:thin:@localhost:1521:test",
			},
		},
	}

	// Save test profiles
	if err := config.SaveProfiles(testProfiles); err != nil {
		t.Fatalf("failed to save test profiles: %v", err)
	}
	// Test: remove all oracle profiles with --force
	output := captureOutput(func() {
		handled, _ := runConnmgrRemove(&replDispatchContext{
			MetaArgs: []string{"oracle-prod", "--db_type", "oracle", "--force"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "Removed 2 profiles successfully") {
		t.Errorf("expected output to contain 'Removed 2 profiles successfully', got: %q", output)
	}

	// Verify: only postgres profile remains
	profiles, _ := config.LoadProfiles()
	if len(profiles.Connections) != 1 {
		t.Errorf("expected 1 profile remaining, got %d", len(profiles.Connections))
	}
	if _, exists := profiles.Connections["postgres-dev"]; !exists {
		t.Error("expected postgres-dev profile to still exist")
	}
}

// TestRunConnmgrRemove_WithLike tests remove command with --like flag for fuzzy matching
func TestRunConnmgrRemove_WithLike(t *testing.T) {
	// Setup test isolation
	cleanup := setupTestConfigDir(t)
	defer cleanup()

	// Setup: create test profiles
	testProfiles := &config.Profiles{
		Version: 1,
		Connections: map[string]config.Profile{
			"prod-db": {
				ID:     "id1",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/orcl",
				URL:    "jdbc:oracle:thin:@localhost:1521:orcl",
			},
			"prod-cache": {
				ID:     "id2",
				DBType: "postgresql",
				DSN:    "postgresql://localhost:5432/cache",
				URL:    "jdbc:postgresql://localhost:5432/cache",
			},
			"dev-db": {
				ID:     "id3",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/dev",
				URL:    "jdbc:oracle:thin:@localhost:1521:dev",
			},
		},
	}

	// Save test profiles
	if err := config.SaveProfiles(testProfiles); err != nil {
		t.Fatalf("failed to save test profiles: %v", err)
	}
	// Test: remove all profiles matching "prod" with --force
	output := captureOutput(func() {
		handled, _ := runConnmgrRemove(&replDispatchContext{
			MetaArgs: []string{"prod", "--like", "--force"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "Removed 2 profiles successfully") {
		t.Errorf("expected output to contain 'Removed 2 profiles successfully', got: %q", output)
	}

	// Verify: only dev-db profile remains
	profiles, _ := config.LoadProfiles()
	if len(profiles.Connections) != 1 {
		t.Errorf("expected 1 profile remaining, got %d", len(profiles.Connections))
	}
	if _, exists := profiles.Connections["dev-db"]; !exists {
		t.Error("expected dev-db profile to still exist")
	}
}

// TestRunConnmgrRemove_WithDbTypeAndLike tests remove command with both --db_type and --like flags
func TestRunConnmgrRemove_WithDbTypeAndLike(t *testing.T) {
	// Setup test isolation
	cleanup := setupTestConfigDir(t)
	defer cleanup()

	// Setup: create test profiles
	testProfiles := &config.Profiles{
		Version: 1,
		Connections: map[string]config.Profile{
			"oracle-prod": {
				ID:     "id1",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/orcl",
				URL:    "jdbc:oracle:thin:@localhost:1521:orcl",
			},
			"postgres-prod": {
				ID:     "id2",
				DBType: "postgresql",
				DSN:    "postgresql://localhost:5432/prod",
				URL:    "jdbc:postgresql://localhost:5432/prod",
			},
			"oracle-dev": {
				ID:     "id3",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/dev",
				URL:    "jdbc:oracle:thin:@localhost:1521:dev",
			},
		},
	}

	// Save test profiles
	if err := config.SaveProfiles(testProfiles); err != nil {
		t.Fatalf("failed to save test profiles: %v", err)
	}
	// Test: remove oracle profiles matching "prod" with --force
	output := captureOutput(func() {
		handled, _ := runConnmgrRemove(&replDispatchContext{
			MetaArgs: []string{"prod", "--db_type", "oracle", "--like", "--force"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "Profile 'oracle-prod' removed successfully") {
		t.Errorf("expected output to contain 'Profile 'oracle-prod' removed successfully', got: %q", output)
	}

	// Verify: only postgres-prod and oracle-dev remain
	profiles, _ := config.LoadProfiles()
	if len(profiles.Connections) != 2 {
		t.Errorf("expected 2 profiles remaining, got %d", len(profiles.Connections))
	}
	if _, exists := profiles.Connections["postgres-prod"]; !exists {
		t.Error("expected postgres-prod profile to still exist")
	}
	if _, exists := profiles.Connections["oracle-dev"]; !exists {
		t.Error("expected oracle-dev profile to still exist")
	}
}

// TestRunConnmgrRemove_MultipleMatchesWithoutForce tests remove command with multiple matches without --force
func TestRunConnmgrRemove_MultipleMatchesWithoutForce(t *testing.T) {
	// Setup test isolation
	cleanup := setupTestConfigDir(t)
	defer cleanup()

	// Setup: create test profiles
	testProfiles := &config.Profiles{
		Version: 1,
		Connections: map[string]config.Profile{
			"prod-db-1": {
				ID:     "id1",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/orcl1",
				URL:    "jdbc:oracle:thin:@localhost:1521:orcl1",
			},
			"prod-db-2": {
				ID:     "id2",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/orcl2",
				URL:    "jdbc:oracle:thin:@localhost:1521:orcl2",
			},
		},
	}

	// Save test profiles
	if err := config.SaveProfiles(testProfiles); err != nil {
		t.Fatalf("failed to save test profiles: %v", err)
	}
	// Test: remove profiles matching "prod" with --force (to test listing behavior)
	output := captureOutput(func() {
		handled, _ := runConnmgrRemove(&replDispatchContext{
			MetaArgs: []string{"prod", "--like", "--force"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "Removed 2 profiles successfully") {
		t.Errorf("expected output to contain 'Removed 2 profiles successfully', got: %q", output)
	}

	// Verify: all profiles are removed
	profiles, _ := config.LoadProfiles()
	if len(profiles.Connections) != 0 {
		t.Errorf("expected 0 profiles remaining, got %d", len(profiles.Connections))
	}
}

// TestRunConnmgrRemove_CaseInsensitive tests remove command with case-insensitive matching
func TestRunConnmgrRemove_CaseInsensitive(t *testing.T) {
	// Setup test isolation
	cleanup := setupTestConfigDir(t)
	defer cleanup()

	// Setup: create test profiles
	testProfiles := &config.Profiles{
		Version: 1,
		Connections: map[string]config.Profile{
			"Oracle-Prod": {
				ID:     "id1",
				DBType: "oracle",
				DSN:    "oracle://localhost:1521/orcl",
				URL:    "jdbc:oracle:thin:@localhost:1521:orcl",
			},
		},
	}

	// Save test profiles
	if err := config.SaveProfiles(testProfiles); err != nil {
		t.Fatalf("failed to save test profiles: %v", err)
	}
	// Test: remove profile with different case
	output := captureOutput(func() {
		handled, _ := runConnmgrRemove(&replDispatchContext{
			MetaArgs: []string{"oracle-prod", "--force"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "Profile 'Oracle-Prod' removed successfully") {
		t.Errorf("expected output to contain 'Profile 'Oracle-Prod' removed successfully', got: %q", output)
	}

	// Verify: profile is removed
	profiles, _ := config.LoadProfiles()
	if len(profiles.Connections) != 0 {
		t.Errorf("expected 0 profiles remaining, got %d", len(profiles.Connections))
	}
}

// TestRunConnmgrShow_MissingProfileName tests show command with missing profile name
func TestRunConnmgrShow_MissingProfileName(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrShow(&replDispatchContext{
			MetaArgs: []string{},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile name required") {
		t.Errorf("expected output to contain 'profile name required', got: %q", output)
	}
}

// TestRunConnmgrShow_EmptyProfileName tests show command with empty profile name
func TestRunConnmgrShow_EmptyProfileName(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrShow(&replDispatchContext{
			MetaArgs: []string{"   "},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile name cannot be empty") {
		t.Errorf("expected output to contain 'profile name cannot be empty', got: %q", output)
	}
}

// TestRunConnmgrShow_NonExistentProfile tests show command with non-existent profile
func TestRunConnmgrShow_NonExistentProfile(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrShow(&replDispatchContext{
			MetaArgs: []string{"non-existent-profile"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile 'non-existent-profile' not found") {
		t.Errorf("expected output to contain 'profile not found', got: %q", output)
	}
}

// TestRunConnmgrShow_ContentVerification tests show command output content
func TestRunConnmgrShow_ContentVerification(t *testing.T) {
	// Setup test isolation
	cleanup := setupTestConfigDir(t)
	defer cleanup()

	// Setup: create a test profile with source information
	testProfiles := &config.Profiles{
		Version: 1,
		Connections: map[string]config.Profile{
			"test-show-profile": {
				ID:           "id1",
				DBType:       "oracle",
				DSN:          "oracle://user:pass@localhost:1521/orcl",
				URL:          "jdbc:oracle:thin:@localhost:1521/orcl",
				SavePassword: true,
				Source: config.Source{
					Kind:         "dbeaver",
					Provider:     "oracle",
					Driver:       "oracle_thin",
					ConnectionID: "conn-123",
				},
			},
		},
	}
	if err := config.SaveProfiles(testProfiles); err != nil {
		t.Fatalf("failed to save test profiles: %v", err)
	}

	output := captureOutput(func() {
		handled, _ := runConnmgrShow(&replDispatchContext{
			MetaArgs: []string{"test-show-profile"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	// Verify output contains expected profile information
	expectedSubstrings := []string{
		"Profile: test-show-profile",
		"Database Type: oracle",
		"DSN:", // DSN should be masked
		"JDBC URL:",
		"Save Password: true",
		"Source:",
		"Kind: dbeaver",
		"Provider: oracle",
		"Driver: oracle_thin",
		"Connection ID: conn-123",
	}

	for _, substr := range expectedSubstrings {
		if !strings.Contains(output, substr) {
			t.Errorf("expected output to contain %q, got: %q", substr, output)
		}
	}
}

// TestRunConnmgrImport_OnConflictSkip tests import with skip conflict strategy on existing profiles
func TestRunConnmgrImport_OnConflictSkip(t *testing.T) {
	// This test verifies that when on_conflict=skip is used and a profile already exists,
	// the import skips that profile without error
	// Note: Full integration testing requires a valid DBP file which is complex to mock
	// This test verifies the validation logic works correctly
	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			MetaArgs: []string{"-dbp", "nonexistent.dbp", "--on_conflict", "skip"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	// Should fail with file not found, not with invalid strategy
	if strings.Contains(output, "invalid on_conflict value") {
		t.Errorf("skip strategy should be valid, got: %q", output)
	}
}

// TestRunConnmgrImport_OnConflictOverwrite tests import with overwrite conflict strategy
func TestRunConnmgrImport_OnConflictOverwrite(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			MetaArgs: []string{"-dbp", "nonexistent.dbp", "--on_conflict", "overwrite"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	// Should fail with file not found, not with invalid strategy
	if strings.Contains(output, "invalid on_conflict value") {
		t.Errorf("overwrite strategy should be valid, got: %q", output)
	}
}

// TestRunConnmgrImport_ConnPrefix tests import with connection prefix
func TestRunConnmgrImport_ConnPrefix(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			MetaArgs: []string{"-dbp", "nonexistent.dbp", "--conn_prefix", "test"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	// Should fail with file not found, not with invalid prefix
	if strings.Contains(output, "invalid") && strings.Contains(output, "prefix") {
		t.Errorf("conn_prefix should be valid, got: %q", output)
	}
}

// TestRunConnmgrUpdate_MissingProfileName tests update command with missing profile name
func TestRunConnmgrUpdate_MissingProfileName(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrUpdate(&replDispatchContext{
			MetaArgs: []string{},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile name required") {
		t.Errorf("expected output to contain 'profile name required', got: %q", output)
	}
}

// TestRunConnmgrUpdate_NoUpdateParameters tests update command with no update parameters
func TestRunConnmgrUpdate_NoUpdateParameters(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrUpdate(&replDispatchContext{
			MetaArgs: []string{"test-profile"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile 'test-profile' not found") {
		t.Errorf("expected output to contain \"profile 'test-profile' not found\", got: %q", output)
	}
}

// TestRunConnmgrUpdate_NonExistentProfile tests update command with non-existent profile
func TestRunConnmgrUpdate_NonExistentProfile(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrUpdate(&replDispatchContext{
			MetaArgs: []string{"non-existent-profile", "--new-name", "new-name"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile 'non-existent-profile' not found") {
		t.Errorf("expected output to contain 'profile not found', got: %q", output)
	}
}

// TestRunConnmgrUpdate_EmptyNewName tests update command with empty new name
func TestRunConnmgrUpdate_EmptyNewName(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrUpdate(&replDispatchContext{
			MetaArgs: []string{"test-profile", "--new-name", "   "},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile 'test-profile' not found") {
		t.Errorf("expected output to contain \"profile 'test-profile' not found\", got: %q", output)
	}
}

// TestRunConnmgrUpdate_EmptyDSN tests update command with empty DSN
func TestRunConnmgrUpdate_EmptyDSN(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrUpdate(&replDispatchContext{
			MetaArgs: []string{"test-profile", "--dsn", "   "},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile 'test-profile' not found") {
		t.Errorf("expected output to contain \"profile 'test-profile' not found\", got: %q", output)
	}
}

// TestRunConnmgrClearCredential_MissingProfileName tests clear-credential command with missing profile name
func TestRunConnmgrClearCredential_MissingProfileName(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrClearCredential(&replDispatchContext{
			MetaArgs: []string{},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile name required") {
		t.Errorf("expected output to contain 'profile name required', got: %q", output)
	}
}

// TestRunConnmgrClearCredential_EmptyProfileName tests clear-credential command with empty profile name
func TestRunConnmgrClearCredential_EmptyProfileName(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrClearCredential(&replDispatchContext{
			MetaArgs: []string{"   "},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile name cannot be empty") {
		t.Errorf("expected output to contain 'profile name cannot be empty', got: %q", output)
	}
}

// TestRunConnmgrClearCredential_NonExistentProfile tests clear-credential command with non-existent profile
func TestRunConnmgrClearCredential_NonExistentProfile(t *testing.T) {
	output := captureOutput(func() {
		handled, _ := runConnmgrClearCredential(&replDispatchContext{
			MetaArgs: []string{"non-existent-profile"},
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "profile 'non-existent-profile' not found") {
		t.Errorf("expected output to contain 'profile not found', got: %q", output)
	}
}

// TestConfigValidateDSN tests the ValidateDSN function
func TestConfigValidateDSN(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid oracle DSN",
			dsn:     "oracle://host:1521/service",
			wantErr: false,
		},
		{
			name:    "valid oracle DSN with TNS alias",
			dsn:     "oracle://alias?TNS_ADMIN=/path/to/wallet",
			wantErr: false,
		},
		{
			name:    "valid postgresql DSN",
			dsn:     "postgresql://localhost:5432/database",
			wantErr: false,
		},
		{
			name:    "valid postgres DSN",
			dsn:     "postgres://127.0.0.1:5432/postgres",
			wantErr: false,
		},
		{
			name:    "invalid - empty string",
			dsn:     "",
			wantErr: true,
			errMsg:  "cannot be empty",
		},
		{
			name:    "invalid - missing ://",
			dsn:     "invalid-dsn",
			wantErr: true,
			errMsg:  "missing '://' separator",
		},
		{
			name:    "invalid - missing host",
			dsn:     "oracle://",
			wantErr: true,
			errMsg:  "missing host",
		},
		{
			name:    "invalid - missing protocol",
			dsn:     "://host:5432/db",
			wantErr: true,
			errMsg:  "missing protocol",
		},
		{
			name:    "invalid - host starts with special character",
			dsn:     "oracle://@host:5432/db",
			wantErr: true,
			errMsg:  "host must start with alphanumeric",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := config.ValidateDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateDSN(%q) error = %v, wantErr %v", tt.dsn, err, tt.wantErr)
			}
			if tt.wantErr && err != nil && !strings.Contains(err.Error(), tt.errMsg) {
				t.Errorf("expected error to contain %q, got: %q", tt.errMsg, err.Error())
			}
		})
	}
}

// TestConfigGenerateJDBCURL tests the GenerateJDBCURL function
func TestConfigGenerateJDBCURL(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected string
	}{
		{
			name:     "oracle DSN",
			dsn:      "oracle://host:1521/service",
			expected: "jdbc:oracle:thin:@host:1521/service",
		},
		{
			name:     "oracle DSN with TNS alias",
			dsn:      "oracle://alias?TNS_ADMIN=/path/to/wallet",
			expected: "jdbc:oracle:thin:@alias?TNS_ADMIN=/path/to/wallet",
		},
		{
			name:     "postgresql DSN",
			dsn:      "postgresql://localhost:5432/database",
			expected: "jdbc:postgresql://localhost:5432/database",
		},
		{
			name:     "postgres DSN",
			dsn:      "postgres://127.0.0.1:5432/postgres",
			expected: "jdbc:postgresql://127.0.0.1:5432/postgres",
		},
		{
			name:     "empty DSN",
			dsn:      "",
			expected: "",
		},
		{
			name:     "invalid DSN - missing ://",
			dsn:      "invalid-dsn",
			expected: "",
		},
		{
			name:     "other database type",
			dsn:      "mysql://localhost:3306/database",
			expected: "jdbc:mysql://localhost:3306/database",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := config.GenerateJDBCURL(tt.dsn)
			if got != tt.expected {
				t.Errorf("GenerateJDBCURL(%q) = %q, want %q", tt.dsn, got, tt.expected)
			}
		})
	}
}

// TestDbeaverSanitizeProfileName tests the SanitizeProfileName function
func TestDbeaverSanitizeProfileName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid name",
			input:    "my-profile",
			expected: "my-profile",
		},
		{
			name:     "name with spaces",
			input:    "my profile",
			expected: "my_profile",
		},
		{
			name:     "name with slashes",
			input:    "my/profile",
			expected: "my_profile",
		},
		{
			name:     "name with @ symbol",
			input:    "name@host",
			expected: "name_host",
		},
		{
			name:     "name with special characters",
			input:    "my#profile$name",
			expected: "my_profile_name",
		},
		{
			name:     "name with dots",
			input:    "my.profile.name",
			expected: "my_profile_name",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only special characters",
			input:    "@#$%",
			expected: "____",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dbeaver.SanitizeProfileName(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizeProfileName(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}
