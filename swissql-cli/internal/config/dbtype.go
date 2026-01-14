package config

import "strings"

// NormalizeDbType normalizes db_type values into canonical backend dbType strings.
func NormalizeDbType(dbType string) string {
	v := strings.ToLower(strings.TrimSpace(dbType))
	switch v {
	case "postgresql", "pg":
		return "postgres"
	case "yashan", "yasdb":
		return "yashandb"
	case "mssql":
		return "sqlserver"
	case "opengauss":
		return "mogdb"
	case "informix-sqli":
		return "informix"
	default:
		return v
	}
}

// IsBuiltinDbType reports whether dbType is handled as a backend builtin.
func IsBuiltinDbType(dbType string) bool {
	switch NormalizeDbType(dbType) {
	case "oracle", "postgres":
		return true
	default:
		return false
	}
}
