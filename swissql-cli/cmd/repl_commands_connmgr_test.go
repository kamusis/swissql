package cmd

import (
	"bytes"
	"fmt"
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

// TestRunConnmgrImport_InvalidConflictStrategy tests validation of on_conflict parameter
func TestRunConnmgrImport_InvalidConflictStrategy(t *testing.T) {
	// Set global variables
	onConflict = "invalid"

	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			Input: "connmgr import -dbp test.dbp --on_conflict invalid",
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "invalid on_conflict value") {
		t.Errorf("expected output to contain 'invalid on_conflict value', got: %q", output)
	}

	// Reset global variable
	onConflict = ""
}

// TestRunConnmgrImport_MissingDBPFlag tests missing required -dbp flag
func TestRunConnmgrImport_MissingDBPFlag(t *testing.T) {
	// Set global variables - set a valid on_conflict to avoid that validation
	dbpPath = ""
	onConflict = "fail"

	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			Input: "connmgr import --on_conflict fail",
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	if !strings.Contains(output, "required flag") && !strings.Contains(output, "dbp") {
		t.Errorf("expected output to contain 'required flag' or 'dbp', got: %q", output)
	}

	// Reset global variable
	onConflict = ""
}

// TestRunConnmgrImport_ValidFlags tests that valid flags are accepted
func TestRunConnmgrImport_ValidFlags(t *testing.T) {
	// Set global variables
	dbpPath = "test.dbp"
	onConflict = "fail"
	connPrefix = ""
	dryRun = false
	defer func() {
		dbpPath = ""
		onConflict = ""
		connPrefix = ""
		dryRun = false
	}()

	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			Input: "connmgr import -dbp test.dbp --on_conflict fail",
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
			// Set global variables
			dbpPath = "test.dbp"
			onConflict = strategy
			defer func() {
				dbpPath = ""
				onConflict = ""
			}()

			output := captureOutput(func() {
				handled, _ := runConnmgrImport(&replDispatchContext{
					Input: fmt.Sprintf("connmgr import -dbp test.dbp --on_conflict %s", strategy),
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
	// Set global variables
	dbpPath = "test.dbp"
	onConflict = "fail"
	dryRun = true
	defer func() {
		dbpPath = ""
		onConflict = ""
		dryRun = false
	}()

	output := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			Input: "connmgr import -dbp test.dbp --dry_run",
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

// TestRunConnmgrPlaceholderCommands tests placeholder commands
func TestRunConnmgrPlaceholderCommands(t *testing.T) {
	tests := []struct {
		name     string
		fn       func(*replDispatchContext) (bool, bool)
		expected string
	}{
		{
			name:     "remove placeholder",
			fn:       runConnmgrRemove,
			expected: "not yet implemented",
		},
		{
			name:     "show placeholder",
			fn:       runConnmgrShow,
			expected: "not yet implemented",
		},
		{
			name:     "update placeholder",
			fn:       runConnmgrUpdate,
			expected: "not yet implemented",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := captureOutput(func() {
				handled, _ := tt.fn(&replDispatchContext{})
				if !handled {
					t.Error("expected handled to be true")
				}
			})

			if !strings.Contains(output, tt.expected) {
				t.Errorf("expected output to contain %q, got: %q", tt.expected, output)
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

// TestGlobalVariableReset tests that global variables don't cause state pollution
func TestGlobalVariableReset(t *testing.T) {
	// First call with specific values
	dbpPath = "test1.dbp"
	onConflict = "fail"
	connPrefix = "prefix1"
	dryRun = false

	output1 := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			Input: "connmgr import -dbp test1.dbp --on_conflict fail",
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	// Second call with different values
	dbpPath = "test2.dbp"
	onConflict = "skip"
	connPrefix = "prefix2"
	dryRun = true

	output2 := captureOutput(func() {
		handled, _ := runConnmgrImport(&replDispatchContext{
			Input: "connmgr import -dbp test2.dbp --on_conflict skip --dry_run",
		})
		if !handled {
			t.Error("expected handled to be true")
		}
	})

	// Both should fail on file not found, not on state pollution
	if strings.Contains(output1, "invalid on_conflict value") {
		t.Error("first call should not fail with invalid on_conflict")
	}
	if strings.Contains(output2, "invalid on_conflict value") {
		t.Error("second call should not fail with invalid on_conflict")
	}

	// Reset global variables
	dbpPath = ""
	onConflict = ""
	connPrefix = ""
	dryRun = false
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
