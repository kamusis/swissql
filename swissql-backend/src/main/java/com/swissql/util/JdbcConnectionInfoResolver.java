package com.swissql.util;

import com.swissql.driver.DriverManifest;
import com.swissql.driver.DriverRegistry;
import org.springframework.stereotype.Service;

import java.util.Locale;

/**
 * Resolves SwissQL DSN + dbType into a JDBC connection configuration.
 *
 * Built-in dbTypes (Oracle/Postgres) use existing mapping logic.
 * Directory-provided dbTypes use {@code driver.json} to deterministically build the JDBC URL.
 */
@Service
public class JdbcConnectionInfoResolver {

    private final DriverRegistry driverRegistry;

    /**
     * Create a resolver.
     *
     * @param driverRegistry driver registry
     */
    public JdbcConnectionInfoResolver(DriverRegistry driverRegistry) {
        this.driverRegistry = driverRegistry;
    }

    /**
     * Resolve a DSN into JDBC connection info.
     *
     * @param dsn dsn
     * @param dbType dbType
     * @return jdbc connection info
     */
    public JdbcConnectionInfo resolve(String dsn, String dbType) {
        DsnParser.ParsedDsn parsed = DsnParser.parseComponents(dsn, dbType);

        String normalizedDbType = parsed.getDbType() != null ? parsed.getDbType().toLowerCase(Locale.ROOT) : "";

        if ("oracle".equalsIgnoreCase(normalizedDbType)) {
            String jdbcUrl = DsnParser.buildOracleJdbcUrl(parsed.getHost(), parsed.getPort(), parsed.getPath(), parsed.getRawQuery());
            return JdbcConnectionInfo.builder()
                    .url(jdbcUrl)
                    .username(parsed.getUsername())
                    .password(parsed.getPassword())
                    .dbType("oracle")
                    .build();
        }

        if ("postgres".equalsIgnoreCase(normalizedDbType) || "postgresql".equalsIgnoreCase(normalizedDbType)) {
            String jdbcUrl = DsnParser.buildPostgresJdbcUrl(parsed.getHost(), parsed.getPort(), parsed.getPath());
            return JdbcConnectionInfo.builder()
                    .url(jdbcUrl)
                    .username(parsed.getUsername())
                    .password(parsed.getPassword())
                    .dbType("postgres")
                    .build();
        }

        DriverRegistry.Entry entry = driverRegistry.find(normalizedDbType)
                .orElseThrow(() -> new IllegalArgumentException("Unsupported database type: " + normalizedDbType));

        DriverManifest manifest = entry.getManifest();
        if (manifest == null) {
            throw new IllegalArgumentException("Missing driver manifest for dbType: " + normalizedDbType);
        }

        int port = parsed.getPort();
        if (port <= 0) {
            Integer defaultPort = manifest.getDefaultPort();
            if (defaultPort == null || defaultPort <= 0) {
                throw new IllegalArgumentException("Port is required and no defaultPort is configured for dbType: " + normalizedDbType);
            }
            port = defaultPort;
        }

        String jdbcUrl = renderJdbcUrl(manifest.getJdbcUrlTemplate(), parsed.getHost(), port, parsed.getPath());

        return JdbcConnectionInfo.builder()
                .url(jdbcUrl)
                .username(parsed.getUsername())
                .password(parsed.getPassword())
                .dbType(normalizedDbType)
                .build();
    }

    private String renderJdbcUrl(String template, String host, int port, String database) {
        if (template == null || template.isBlank()) {
            throw new IllegalArgumentException("jdbcUrlTemplate is required");
        }
        String db = database != null ? database : "";
        // TODO: Support additional template placeholders for special JDBC parameters
        // - Informix: {server} for INFORMIXSERVER parameter
        // - SQL Server: current template may not match DBeaver format (jdbc:sqlserver://;serverName=host;port=port;databaseName=database)
        // - Sybase: {database} is used as ServiceName query parameter, verify this works correctly
        return template
                .replace("{host}", host != null ? host : "")
                .replace("{port}", String.valueOf(port))
                .replace("{database}", db);
    }
}
