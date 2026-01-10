package client

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestClientDtoJsonTagsAreSnakeCase(t *testing.T) {
	t.Helper()

	// Fail fast if any struct tag under internal/client uses camelCase in json tags.
	// We treat any capital letter inside json:"..." as a violation.
	camelCaseJsonTag := regexp.MustCompile("json:\\\"[^\\\"]*[A-Z][^\\\"]*\\\"")

	root := "."
	entries := 0

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		if filepath.Base(path) == "json_tag_guard_test.go" {
			return nil
		}

		entries++
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if camelCaseJsonTag.Match(b) {
			t.Errorf("camelCase json tag found in %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to scan internal/client: %v", err)
	}
	if entries == 0 {
		t.Fatalf("guardrail scan found no Go files under internal/client")
	}
}
