package cmd

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

func handleReplDriverCommands(cmd *cobra.Command, line *liner.State, historyMode string, input string, c *client.Client) bool {
	_ = cmd
	trimmed := strings.TrimSpace(input)
	lower := strings.ToLower(trimmed)
	if lower != "list drivers" && lower != "list driver" && lower != "reload drivers" && lower != "reload driver" {
		return false
	}

	if shouldRecordHistory(historyMode, input, false) {
		line.AppendHistory(input)
	}

	if lower == "list drivers" || lower == "list driver" {
		start := time.Now()
		drivers, err := c.MetaDrivers()
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}

		rows := make([]map[string]interface{}, 0, len(drivers.Drivers))
		for _, d := range drivers.Drivers {
			var defaultPort interface{}
			if d.DefaultPort != nil {
				defaultPort = *d.DefaultPort
			}
			rows = append(rows, map[string]interface{}{
				"db_type":           d.DbType,
				"source":            d.Source,
				"driver_class":      d.DriverClass,
				"driver_classes":    strings.Join(d.DriverClasses, "\n"),
				"jar_paths":         strings.Join(d.JarPaths, "\n"),
				"jdbc_url_template": d.JdbcUrlTemplate,
				"default_port":      defaultPort,
			})
		}

		resp := &client.ExecuteResponse{
			Type: "tabular",
			Data: client.DataContent{
				Columns: []client.ColumnDefinition{
					{Name: "db_type", Type: "string"},
					{Name: "source", Type: "string"},
					{Name: "driver_class", Type: "string"},
					{Name: "driver_classes", Type: "string"},
					{Name: "jar_paths", Type: "string"},
					{Name: "jdbc_url_template", Type: "string"},
					{Name: "default_port", Type: "number"},
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

	start := time.Now()
	resp, err := c.ReloadDrivers()
	if err != nil {
		fmt.Printf("%v\n", err)
		return true
	}

	if outputFormat == "json" {
		b, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			fmt.Printf("%v\n", err)
			return true
		}
		fmt.Printf("%s\n", string(b))
		return true
	}

	reloaded := ""
	if resp.Reloaded != nil {
		if b, err := json.Marshal(resp.Reloaded); err == nil {
			reloaded = string(b)
		}
	}

	out := &client.ExecuteResponse{
		Type: "tabular",
		Data: client.DataContent{
			Columns: []client.ColumnDefinition{
				{Name: "status", Type: "string"},
				{Name: "reloaded", Type: "json"},
			},
			Rows: []map[string]interface{}{
				{"status": resp.Status, "reloaded": reloaded},
			},
		},
		Metadata: client.ResponseMetadata{
			DurationMs:   int(time.Since(start).Milliseconds()),
			RowsAffected: 1,
			Truncated:    false,
		},
	}
	renderResponse(cmd, out)
	return true
}
