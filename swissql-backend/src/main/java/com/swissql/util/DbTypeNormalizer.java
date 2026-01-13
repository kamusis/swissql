package com.swissql.util;

import java.util.Locale;
import java.util.Map;

/**
 * Normalizes incoming dbType (and aliases) into canonical dbType strings.
 *
 * This must run before builtin checks and before {@code DriverRegistry.find(...)}.
 */
public final class DbTypeNormalizer {

    private static final Map<String, String> ALIASES = Map.ofEntries(
            Map.entry("postgresql", "postgres"),
            Map.entry("pg", "postgres"),
            Map.entry("yashan", "yashandb"),
            Map.entry("yasdb", "yashandb"),
            Map.entry("oracle", "oracle"),
            Map.entry("sqlserver", "sqlserver"),
            Map.entry("mssql", "sqlserver"),
            Map.entry("mysql", "mysql"),
            // Additional practical aliases used by common JDBC drivers.
            Map.entry("opengauss", "mogdb"),
            Map.entry("informix-sqli", "informix")
    );

    private DbTypeNormalizer() {
    }

    /**
     * Normalize dbType.
     *
     * @param dbType incoming dbType
     * @return normalized dbType (lowercased + alias mapping)
     */
    public static String normalize(String dbType) {
        if (dbType == null) {
            return "";
        }
        String v = dbType.trim().toLowerCase(Locale.ROOT);
        if (v.isBlank()) {
            return "";
        }
        return ALIASES.getOrDefault(v, v);
    }
}
