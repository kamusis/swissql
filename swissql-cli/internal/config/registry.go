package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const registryVersion = 1

// SessionEntry represents a named backend session that can be attached/detached like tmux.
//
// Note: We intentionally do not persist the raw DSN to avoid storing credentials on disk.
// Use DsnMasked for display/debug only.
type SessionEntry struct {
	Name       string    `json:"name"`
	SessionId  string    `json:"session_id"`
	ServerURL  string    `json:"server_url"`
	DbType     string    `json:"db_type"`
	DsnMasked  string    `json:"dsn_masked,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	LastUsedAt time.Time `json:"last_used_at"`
}

// Registry stores multiple named sessions.
type Registry struct {
	Version  int                     `json:"version"`
	Sessions map[string]SessionEntry `json:"sessions"`
}

// GetRegistryPath returns the path to the local registry file.
func GetRegistryPath() (string, error) {
	dir, err := GetConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "registry.json"), nil
}

// LoadRegistry loads the local registry from disk.
func LoadRegistry() (*Registry, error) {
	path, err := GetRegistryPath()
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return &Registry{Version: registryVersion, Sessions: map[string]SessionEntry{}}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var reg Registry
	if err := json.Unmarshal(data, &reg); err != nil {
		return nil, err
	}
	if reg.Sessions == nil {
		reg.Sessions = map[string]SessionEntry{}
	}
	if reg.Version == 0 {
		reg.Version = registryVersion
	}
	return &reg, nil
}

// SaveRegistry atomically writes the registry file.
func SaveRegistry(reg *Registry) error {
	path, err := GetRegistryPath()
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}

	_ = os.Remove(path)
	return os.Rename(tmp, path)
}

// UpsertSession inserts or updates a session entry.
func (reg *Registry) UpsertSession(entry SessionEntry) {
	if reg.Sessions == nil {
		reg.Sessions = map[string]SessionEntry{}
	}
	reg.Sessions[entry.Name] = entry
}

// RemoveSession deletes a named session entry.
func (reg *Registry) RemoveSession(name string) {
	if reg.Sessions == nil {
		return
	}
	delete(reg.Sessions, name)
}

// GetSession returns a named session entry.
func (reg *Registry) GetSession(name string) (SessionEntry, bool) {
	if reg.Sessions == nil {
		return SessionEntry{}, false
	}
	entry, ok := reg.Sessions[name]
	return entry, ok
}

// MaskDsn removes password from DSN for safe display.
func MaskDsn(raw string) string {
	if raw == "" {
		return ""
	}

	// 1. Try standard parsing first
	u, err := url.Parse(raw)
	if err == nil && u.Host != "" && !strings.Contains(u.User.String(), "#") {
		if u.User != nil {
			username := u.User.Username()
			u.User = url.User(username)
		}
		return u.String()
	}

	// 2. Manual fallback for DSNs with special characters (like '#') in passwords
	// Example: postgres://user:p#ss@host:port/db

	// Find the last '@' which separates userinfo from host
	atIndex := strings.LastIndex(raw, "@")
	if atIndex == -1 {
		// No userinfo, or very malformed. Try to just return the string if no sensitive parts seen.
		if !strings.Contains(raw, "://") {
			return raw
		}
		return raw
	}

	prefix := raw[:atIndex]   // e.g. postgres://user:p#ss
	suffix := raw[atIndex+1:] // e.g. host:port/db

	// Mask the password in the prefix
	protoIndex := strings.Index(prefix, "://")
	if protoIndex != -1 {
		protocol := prefix[:protoIndex+3]
		userPart := prefix[protoIndex+3:]
		colonIndex := strings.Index(userPart, ":")
		if colonIndex != -1 {
			username := userPart[:colonIndex]
			return protocol + username + "@" + suffix
		}
		return protocol + userPart + "@" + suffix
	}

	return "masked://" + suffix
}

// GetRemoteHost returns the remote hostname or TNS alias from the DSN.
func (e SessionEntry) GetRemoteHost() string {
	if e.DsnMasked == "" {
		return "none"
	}

	// Try parsing the masked DSN
	u, err := url.Parse(e.DsnMasked)
	if err == nil && u.Host != "" {
		host := u.Hostname()
		if host == "" {
			host = u.Host
		}
		if host != "" {
			return host
		}
	}

	// Manual fallback for masked DSN if parsing fails
	// Example: postgres://user@host:port/db or masked://host:port/db
	atIndex := strings.LastIndex(e.DsnMasked, "@")
	hostPart := e.DsnMasked
	if atIndex != -1 {
		hostPart = e.DsnMasked[atIndex+1:]
	} else {
		protoIndex := strings.Index(e.DsnMasked, "://")
		if protoIndex != -1 {
			hostPart = e.DsnMasked[protoIndex+3:]
		}
	}

	// hostPart might be "host:port/path"
	slashIndex := strings.Index(hostPart, "/")
	if slashIndex != -1 {
		hostPart = hostPart[:slashIndex]
	}
	colonIndex := strings.Index(hostPart, ":")
	if colonIndex != -1 {
		hostPart = hostPart[:colonIndex]
	}

	if hostPart == "" {
		return "unknown"
	}
	return hostPart
}

// ResolveActiveSession resolves the session to use for a command.
//
// Resolution order:
// 1) If name is provided, load from registry.
// 2) Else if config.current_name is set, load from registry.
// 3) Else fallback to legacy config.session_id fields.
func ResolveActiveSession(name string) (SessionEntry, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return SessionEntry{}, err
	}

	reg, err := LoadRegistry()
	if err != nil {
		return SessionEntry{}, err
	}

	resolvedName := name
	if resolvedName == "" {
		resolvedName = cfg.CurrentName
	}

	if resolvedName != "" {
		entry, ok := reg.GetSession(resolvedName)
		if !ok {
			return SessionEntry{}, fmt.Errorf("session not found in registry: %s", resolvedName)
		}
		return entry, nil
	}

	if cfg.SessionId == "" {
		return SessionEntry{}, errors.New("no active session. Please run 'swissql connect' first")
	}
	return SessionEntry{
		Name:      "",
		SessionId: cfg.SessionId,
		ServerURL: cfg.ServerURL,
		DbType:    cfg.DbType,
		DsnMasked: MaskDsn(cfg.Dsn),
	}, nil
}

// MostRecentSessionName returns the name of the most recently used session.
// If LastUsedAt is zero, CreatedAt is used as a fallback.
func (reg *Registry) MostRecentSessionName() (string, bool) {
	if reg == nil || len(reg.Sessions) == 0 {
		return "", false
	}

	names := make([]string, 0, len(reg.Sessions))
	for name := range reg.Sessions {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool {
		a := reg.Sessions[names[i]]
		b := reg.Sessions[names[j]]

		ta := a.LastUsedAt
		if ta.IsZero() {
			ta = a.CreatedAt
		}
		tb := b.LastUsedAt
		if tb.IsZero() {
			tb = b.CreatedAt
		}
		return ta.After(tb)
	})

	return names[0], true
}

// TouchSession updates last_used_at for a named session in the registry.
func TouchSession(name string) error {
	if name == "" {
		return nil
	}

	reg, err := LoadRegistry()
	if err != nil {
		return err
	}

	entry, ok := reg.GetSession(name)
	if !ok {
		return fmt.Errorf("session not found in registry: %s", name)
	}
	entry.LastUsedAt = time.Now()
	reg.UpsertSession(entry)
	return SaveRegistry(reg)
}
