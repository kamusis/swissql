package cmd

import (
	"bytes"
	"strings"
	"testing"
)

// executeRootCmd runs the cobra root command with the given args and captures stdout/stderr.
func executeRootCmd(t *testing.T, args ...string) (stdout string, stderr string, err error) {
	t.Helper()

	// Cobra commands are global singletons in this package; avoid parallel execution.
	outBuf := new(bytes.Buffer)
	errBuf := new(bytes.Buffer)

	rootCmd.SetOut(outBuf)
	rootCmd.SetErr(errBuf)
	rootCmd.SetArgs(args)

	err = rootCmd.Execute()
	return outBuf.String(), errBuf.String(), err
}

// TestCLI_HelpSmoke verifies that the CLI command tree is wired and can render help.
func TestCLI_HelpSmoke(t *testing.T) {
	stdout, _, err := executeRootCmd(t, "--help")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !strings.Contains(stdout, "Usage:") || !strings.Contains(stdout, "swissql [command]") {
		t.Fatalf("expected help output to include usage for swissql, got: %q", stdout)
	}
	if !strings.Contains(strings.ToLower(stdout), "run without a subcommand") {
		t.Fatalf("expected help output to describe default REPL behavior, got: %q", stdout)
	}
}

// TestCLI_SubcommandHelpSmoke verifies key subcommands can render help without backend access.
func TestCLI_SubcommandHelpSmoke(t *testing.T) {
	cases := []struct {
		name        string
		args        []string
		wantSubstrs []string
	}{
		{
			name: "connect_help",
			args: []string{"connect", "--help"},
			wantSubstrs: []string{
				"connect",
			},
		},
		{
			name: "ls_help",
			args: []string{"ls", "--help"},
			wantSubstrs: []string{
				"ls",
			},
		},
		{
			name: "attach_help",
			args: []string{"attach", "--help"},
			wantSubstrs: []string{
				"attach",
			},
		},
		{
			name: "kill_help",
			args: []string{"kill", "--help"},
			wantSubstrs: []string{
				"kill",
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			stdout, _, err := executeRootCmd(t, tc.args...)
			if err != nil {
				t.Fatalf("expected no error, got: %v", err)
			}
			for _, sub := range tc.wantSubstrs {
				if !strings.Contains(strings.ToLower(stdout), strings.ToLower(sub)) {
					t.Fatalf("expected help output to contain %q, got: %q", sub, stdout)
				}
			}
		})
	}
}
