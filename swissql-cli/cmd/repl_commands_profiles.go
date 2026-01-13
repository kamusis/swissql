package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

// handleReplProfileCommands handles REPL commands for listing profiles.
func handleReplProfileCommands(cmd *cobra.Command, line *liner.State, historyMode string, input string) bool {
	_ = cmd

	trimmed := strings.TrimSpace(input)
	lower := strings.ToLower(trimmed)
	if lower != "list profiles" && lower != "list profile" && !strings.HasPrefix(lower, "list profiles ") && !strings.HasPrefix(lower, "list profile ") {
		return false
	}

	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	filters, err := parseReplFilters(trimmed)
	if err != nil {
		fmt.Printf("%v\n", err)
		return true
	}
	if err := validateProfileFilterKeys(filters); err != nil {
		fmt.Printf("%v\n", err)
		return true
	}

	start := time.Now()
	profiles, err := config.LoadProfiles()
	if err != nil {
		fmt.Printf("%v\n", err)
		return true
	}

	rows := make([]map[string]interface{}, 0, len(profiles.Connections))
	for name, p := range profiles.Connections {
		if !matchProfileFilters(name, p, filters) {
			continue
		}

		rows = append(rows, map[string]interface{}{
			"name":                 name,
			"id":                   p.ID,
			"db_type":              p.DBType,
			"dsn":                  config.MaskDsn(p.DSN),
			"url":                  maskJdbcUrl(p.URL),
			"save_password":        p.SavePassword,
			"source_kind":          p.Source.Kind,
			"source_provider":      p.Source.Provider,
			"source_driver":        p.Source.Driver,
			"source_connection_id": p.Source.ConnectionID,
		})
	}

	resp := &client.ExecuteResponse{
		Type: "tabular",
		Data: client.DataContent{
			Columns: []client.ColumnDefinition{
				{Name: "name", Type: "string"},
				{Name: "id", Type: "string"},
				{Name: "db_type", Type: "string"},
				{Name: "dsn", Type: "string"},
				{Name: "url", Type: "string"},
				{Name: "save_password", Type: "bool"},
				{Name: "source_kind", Type: "string"},
				{Name: "source_provider", Type: "string"},
				{Name: "source_driver", Type: "string"},
				{Name: "source_connection_id", Type: "string"},
			},
			Rows: rows,
		},
		Metadata: client.ResponseMetadata{
			DurationMs:   int(time.Since(start).Milliseconds()),
			RowsAffected: len(rows),
			Truncated:    false,
		},
	}

	renderResponse(cmd, resp)
	return true
}

func validateProfileFilterKeys(filters map[string][]string) error {
	if len(filters) == 0 {
		return nil
	}

	supported := map[string]struct{}{
		"name":                 {},
		"id":                   {},
		"db_type":              {},
		"dsn":                  {},
		"url":                  {},
		"save_password":        {},
		"source.kind":          {},
		"source.provider":      {},
		"source.driver":        {},
		"source.connection_id": {},
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

// parseReplFilters parses one or more --filter key=value tokens from a REPL command line.
func parseReplFilters(input string) (map[string][]string, error) {
	fields := strings.Fields(strings.TrimSpace(input))
	if len(fields) < 2 {
		return map[string][]string{}, nil
	}

	filters := map[string][]string{}
	for i := 2; i < len(fields); i++ {
		if fields[i] != "--filter" {
			continue
		}
		if i+1 >= len(fields) {
			return nil, fmt.Errorf("--filter requires key=value")
		}
		kv := strings.TrimSpace(strings.Trim(fields[i+1], "\"'"))
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

// matchProfileFilters applies composite filtering (AND across keys, OR across repeated values for a key).
func matchProfileFilters(name string, p config.Profile, filters map[string][]string) bool {
	if len(filters) == 0 {
		return true
	}

	getField := func(key string) string {
		switch key {
		case "name":
			return name
		case "id":
			return p.ID
		case "db_type":
			return p.DBType
		case "dsn":
			return p.DSN
		case "url":
			return p.URL
		case "save_password":
			if p.SavePassword {
				return "true"
			}
			return "false"
		case "source.kind":
			return p.Source.Kind
		case "source.provider":
			return p.Source.Provider
		case "source.driver":
			return p.Source.Driver
		case "source.connection_id":
			return p.Source.ConnectionID
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

// maskJdbcUrl attempts to mask credentials in a JDBC URL for display.
func maskJdbcUrl(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "jdbc:") {
		return raw
	}

	rest := strings.TrimPrefix(raw, "jdbc:")
	if strings.Contains(rest, "://") {
		return "jdbc:" + config.MaskDsn(rest)
	}
	return raw
}
