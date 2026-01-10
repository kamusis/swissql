package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func readFileContent(baseDir string, path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("file path is required")
	}
	p := path
	if baseDir != "" && !filepath.IsAbs(path) {
		p = filepath.Join(baseDir, path)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func trimTrailingSemicolon(s string) string {
	t := strings.TrimSpace(s)
	if strings.HasSuffix(t, ";") {
		return strings.TrimSpace(strings.TrimSuffix(t, ";"))
	}
	return t
}

func isMetaCommandStart(s string) bool {
	if s == "" {
		return false
	}
	if strings.HasPrefix(s, "\\") || strings.HasPrefix(s, "@") {
		return true
	}

	fields := strings.Fields(s)
	if len(fields) == 0 {
		return false
	}
	firstWord := strings.ToLower(fields[0])
	switch firstWord {
	case "desc", "desc+", "explain", "conninfo":
		return true
	default:
		return false
	}
}

func parseMetaCommand(input string) (string, []string) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", nil
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return "", nil
	}
	cmd := fields[0]
	args := []string{}
	if len(fields) > 1 {
		args = fields[1:]
	}
	return cmd, args
}

func shouldRecordHistory(mode string, input string, isSQL bool) bool {
	if isSQL {
		return true
	}

	s := strings.TrimSpace(input)
	if s == "" {
		return false
	}

	sLower := strings.ToLower(s)

	switch mode {
	case "all":
		return true
	case "sql_only":
		return false
	case "safe_only":
		// Blacklist-only policy for maintainability: record everything except a few
		// potentially sensitive or noisy commands.
		if strings.HasPrefix(sLower, "/ai") || strings.HasPrefix(sLower, "/context") {
			return false
		}
		if strings.HasPrefix(s, "@") || strings.HasPrefix(s, "\\i") {
			return false
		}
		return true
	default:
		return false
	}
}
