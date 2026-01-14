package dbeaver

import (
	"archive/zip"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/kamusis/swissql/swissql-cli/internal/config"
)

// ParseDBP parses a DBeaver .dbp file and returns the archive structure
func ParseDBP(path string) (*DBPArchive, error) {
	reader, err := zip.OpenReader(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open dbp file: %w", err)
	}
	defer reader.Close()

	archive := &DBPArchive{}

	// Extract meta.xml
	for _, file := range reader.File {
		if file.Name == "meta.xml" {
			rc, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("failed to open meta.xml: %w", err)
			}
			archive.MetaXML, err = io.ReadAll(rc)
			rc.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to read meta.xml: %w", err)
			}
			break
		}
	}

	// Extract data-sources.json
	dataSources, err := ExtractDataSources(&reader.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to extract data-sources.json: %w", err)
	}
	archive.DataSources = dataSources

	return archive, nil
}

// ExtractDataSources extracts and parses data-sources.json from a zip archive
func ExtractDataSources(zipReader *zip.Reader) (*DataSources, error) {
	for _, file := range zipReader.File {
		if strings.HasSuffix(file.Name, "data-sources.json") {
			rc, err := file.Open()
			if err != nil {
				return nil, fmt.Errorf("failed to open data-sources.json: %w", err)
			}
			defer rc.Close()

			data, err := io.ReadAll(rc)
			if err != nil {
				return nil, fmt.Errorf("failed to read data-sources.json: %w", err)
			}

			var dataSources DataSources
			if err := json.Unmarshal(data, &dataSources); err != nil {
				return nil, fmt.Errorf("failed to parse data-sources.json: %w", err)
			}

			return &dataSources, nil
		}
	}

	return nil, fmt.Errorf("data-sources.json not found in archive")
}

// ConvertConnection converts a DBeaver connection to a SwissQL profile
func ConvertConnection(conn *DBeaverConnection, prefix string) (*config.Profile, error) {
	dbType := InferDBType(conn.Provider, conn.Configuration.URL)
	dsn := JDBCToSwissQLDSN(dbType, conn.Configuration.URL)

	profileID := fmt.Sprintf("%s_%s", dbType, GenerateUUID())

	profile := &config.Profile{
		ID:           profileID,
		DBType:       dbType,
		DSN:          dsn,
		URL:          conn.Configuration.URL,
		SavePassword: conn.SavePassword,
		Source: config.Source{
			Kind:         "dbeaver",
			Provider:     conn.Provider,
			Driver:       conn.Driver,
			ConnectionID: "", // Will be set by caller
		},
	}

	return profile, nil
}

// InferDBType infers the SwissQL db_type from provider and JDBC URL
func InferDBType(provider, jdbcURL string) string {
	provider = strings.ToLower(provider)
	provider = strings.TrimSpace(provider)

	// Prefer extracting protocol from JDBC URL when present.
	// This avoids relying on provider labels that can be misleading (e.g., Sybase using provider=mssql).
	if strings.HasPrefix(jdbcURL, "jdbc:") {
		urlWithoutJDBC := strings.TrimPrefix(jdbcURL, "jdbc:")
		if idx := strings.Index(urlWithoutJDBC, ":"); idx != -1 {
			protocol := strings.TrimSpace(urlWithoutJDBC[:idx])
			if protocol != "" {
				return config.NormalizeDbType(protocol)
			}
		}
	}

	// Fallback to provider.
	return config.NormalizeDbType(provider)
}

// JDBCToSwissQLDSN converts a JDBC URL to a SwissQL DSN format
// Example: jdbc:postgresql://host:port/database → postgresql://host:port/database
// Example: jdbc:oracle:thin:@//host:port/service → oracle://host:port/service
// Example: jdbc:oracle:thin:@alias?TNS_ADMIN=path → oracle://alias?TNS_ADMIN=path
func JDBCToSwissQLDSN(dbType, jdbcURL string) string {
	// Remove jdbc: prefix
	if !strings.HasPrefix(jdbcURL, "jdbc:") {
		return ""
	}

	urlWithoutJDBC := strings.TrimPrefix(jdbcURL, "jdbc:")

	normalizedDbType := config.NormalizeDbType(dbType)

	// Find protocol (first colon) and // separator
	protocolIdx := strings.Index(urlWithoutJDBC, ":")
	separatorIdx := strings.Index(urlWithoutJDBC, "//")

	if protocolIdx == -1 {
		return ""
	}

	protocol := urlWithoutJDBC[:protocolIdx]

	if separatorIdx != -1 {
		// Standard format with //: jdbc:protocol:driver://authority/path
		rest := urlWithoutJDBC[separatorIdx:] // Keep "//" and everything after
		return normalizedDbType + ":" + rest
	}

	// No // separator, handle special cases (e.g., Oracle TNS alias, Sybase Tds)
	// Find @ symbol which separates driver from authority
	atIdx := strings.Index(urlWithoutJDBC[protocolIdx+1:], "@")
	if atIdx != -1 {
		// jdbc:oracle:thin:@alias → oracle://alias
		rest := urlWithoutJDBC[protocolIdx+1+atIdx+1:] // Skip protocol, driver, and @
		return normalizedDbType + "://" + rest
	}

	// Handle Sybase Tds: format: jdbc:sybase:Tds:host:port?ServiceName=database
	if strings.Contains(urlWithoutJDBC[protocolIdx+1:], "Tds:") {
		// Remove Tds: prefix
		rest := strings.Replace(urlWithoutJDBC[protocolIdx+1:], "Tds:", "", 1)
		return normalizedDbType + "://" + rest
	}

	// Fallback: just use everything after protocol
	rest := urlWithoutJDBC[protocolIdx+1:]
	_ = protocol
	return normalizedDbType + "://" + rest
}

// GenerateUUID generates a new UUID string using crypto/rand
func GenerateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// SanitizeProfileName sanitizes a profile name for use as a key
func SanitizeProfileName(name string) string {
	// Replace invalid characters with underscores
	name = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, name)
	return name
}
