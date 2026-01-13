package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// Profiles represents the connections.json structure
type Profiles struct {
	Version     int                `json:"version"`
	Connections map[string]Profile `json:"connections"`
}

// Profile represents a SwissQL connection profile
type Profile struct {
	ID           string `json:"id"`
	DBType       string `json:"db_type"`
	DSN          string `json:"dsn"`
	URL          string `json:"url"`
	SavePassword bool   `json:"save_password"`
	Source       Source `json:"source"`
}

// Source represents the provenance of a profile
type Source struct {
	Kind         string `json:"kind"`
	Provider     string `json:"provider"`
	Driver       string `json:"driver"`
	ConnectionID string `json:"connection_id"`
}

// ConflictStrategy defines how to handle profile name conflicts
type ConflictStrategy string

const (
	ConflictFail      ConflictStrategy = "fail"
	ConflictSkip      ConflictStrategy = "skip"
	ConflictOverwrite ConflictStrategy = "overwrite"
)

// LoadProfiles loads profiles from ~/.swissql/connections.json
func LoadProfiles() (*Profiles, error) {
	configDir, err := GetConfigDir()
	if err != nil {
		return nil, err
	}

	path := filepath.Join(configDir, "connections.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Return empty profiles if file doesn't exist
			return &Profiles{
				Version:     1,
				Connections: make(map[string]Profile),
			}, nil
		}
		return nil, err
	}

	var profiles Profiles
	if err := json.Unmarshal(data, &profiles); err != nil {
		return nil, err
	}

	return &profiles, nil
}

// SaveProfiles saves profiles to ~/.swissql/connections.json
func SaveProfiles(profiles *Profiles) error {
	configDir, err := GetConfigDir()
	if err != nil {
		return err
	}

	if err := os.MkdirAll(configDir, 0755); err != nil {
		return err
	}

	path := filepath.Join(configDir, "connections.json")
	data, err := json.MarshalIndent(profiles, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0600)
}

// GetProfile retrieves a profile by name
func GetProfile(name string) (*Profile, error) {
	profiles, err := LoadProfiles()
	if err != nil {
		return nil, err
	}

	profile, exists := profiles.Connections[name]
	if !exists {
		return nil, nil
	}

	return &profile, nil
}

// AddProfile adds a new profile with conflict handling
func AddProfile(name string, profile *Profile, strategy ConflictStrategy) (bool, error) {
	profiles, err := LoadProfiles()
	if err != nil {
		return false, err
	}

	_, exists := profiles.Connections[name]
	if exists {
		switch strategy {
		case ConflictFail:
			return false, nil
		case ConflictSkip:
			return false, nil
		case ConflictOverwrite:
			profiles.Connections[name] = *profile
			return true, SaveProfiles(profiles)
		}
	}

	profiles.Connections[name] = *profile
	return true, SaveProfiles(profiles)
}
