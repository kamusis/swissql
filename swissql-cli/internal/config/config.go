package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type Config struct {
	CurrentName     string `json:"current_name"`
	DisplayWide     bool   `json:"display_wide"`
	DisplayExpanded bool   `json:"display_expanded"`
	OutputFormat    string `json:"output_format"`
	History         struct {
		Mode string `json:"mode"`
	} `json:"history"`
	Display struct {
		MaxColWidth   int `json:"max_col_width"`
		MaxQueryWidth int `json:"max_query_width"`
	} `json:"display"`
}

func normalizeOutputFormat(format string) string {
	switch format {
	case "table", "csv", "tsv", "json":
		return format
	default:
		return "table"
	}
}

func normalizeHistoryMode(mode string) string {
	switch mode {
	case "sql_only", "safe_only", "all":
		return mode
	default:
		return "safe_only"
	}
}

func GetConfigDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	configDir := filepath.Join(home, ".swissql")
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		if err := os.MkdirAll(configDir, 0755); err != nil {
			return "", err
		}
	}
	return configDir, nil
}

func GetConfigPath() (string, error) {
	dir, err := GetConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.json"), nil
}

func GetHistoryPath() (string, error) {
	dir, err := GetConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "history"), nil
}

func LoadConfig() (*Config, error) {
	path, err := GetConfigPath()
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		cfg := &Config{}
		cfg.DisplayWide = false
		cfg.DisplayExpanded = false
		cfg.OutputFormat = "table"
		cfg.History.Mode = "safe_only"
		cfg.Display.MaxColWidth = 32
		cfg.Display.MaxQueryWidth = 60
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// Defaults for newly introduced fields
	cfg.History.Mode = normalizeHistoryMode(cfg.History.Mode)
	cfg.OutputFormat = normalizeOutputFormat(cfg.OutputFormat)
	if cfg.Display.MaxColWidth == 0 {
		cfg.Display.MaxColWidth = 32
	}
	if cfg.Display.MaxQueryWidth == 0 {
		cfg.Display.MaxQueryWidth = 60
	}
	return &cfg, nil
}

func SaveConfig(cfg *Config) error {
	path, err := GetConfigPath()
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}
