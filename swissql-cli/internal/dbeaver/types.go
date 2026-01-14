package dbeaver

// DBPArchive represents a parsed DBeaver .dbp archive
type DBPArchive struct {
	MetaXML     []byte
	DataSources *DataSources
}

// DataSources represents the data-sources.json structure
type DataSources struct {
	Connections map[string]DBeaverConnection `json:"connections"`
}

// DBeaverConnection represents a single DBeaver connection
type DBeaverConnection struct {
	Name          string           `json:"name"`
	Provider      string           `json:"provider"`
	Driver        string           `json:"driver"`
	SavePassword  bool             `json:"save-password"`
	Configuration ConnectionConfig `json:"configuration"`
}

// ConnectionConfig represents the connection configuration
type ConnectionConfig struct {
	Host              string `json:"host"`
	Port              string `json:"port"`
	Database          string `json:"database"`
	URL               string `json:"url"`
	ConfigurationType string `json:"configurationType"`
}

// ImportResult represents the result of an import operation
type ImportResult struct {
	Discovered  int
	Created     int
	Skipped     int
	Overwritten int
	Errors      []ImportError
}

// ImportError represents an error during import
type ImportError struct {
	ConnectionName string
	Message        string
}
