package com.swissql.util;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class DsnParser {

    /**
     * Parsed DSN components.
     */
    public static class ParsedDsn {
        private final String scheme;
        private final String dbType;
        private final String username;
        private final String password;
        private final String host;
        private final int port;
        private final String path;
        private final String rawQuery;

        /**
         * Create a parsed DSN.
         *
         * @param scheme scheme
         * @param dbType dbType
         * @param username username
         * @param password password
         * @param host host
         * @param port port
         * @param path path
         * @param rawQuery raw query
         */
        public ParsedDsn(
                String scheme,
                String dbType,
                String username,
                String password,
                String host,
                int port,
                String path,
                String rawQuery
        ) {
            this.scheme = scheme;
            this.dbType = dbType;
            this.username = username;
            this.password = password;
            this.host = host;
            this.port = port;
            this.path = path;
            this.rawQuery = rawQuery;
        }

        /**
         * Get scheme.
         *
         * @return scheme
         */
        public String getScheme() {
            return scheme;
        }

        /**
         * Get dbType.
         *
         * @return dbType
         */
        public String getDbType() {
            return dbType;
        }

        /**
         * Get username.
         *
         * @return username
         */
        public String getUsername() {
            return username;
        }

        /**
         * Get password.
         *
         * @return password
         */
        public String getPassword() {
            return password;
        }

        /**
         * Get host.
         *
         * @return host
         */
        public String getHost() {
            return host;
        }

        /**
         * Get port.
         *
         * @return port
         */
        public int getPort() {
            return port;
        }

        /**
         * Get path.
         *
         * @return path
         */
        public String getPath() {
            return path;
        }

        /**
         * Get raw query string.
         *
         * @return query
         */
        public String getRawQuery() {
            return rawQuery;
        }
    }

    /**
     * Parse DSN into reusable components. This method does not perform dbType-specific JDBC URL mapping.
     *
     * @param dsn dsn
     * @param dbType dbType
     * @return parsed DSN
     */
    public static ParsedDsn parseComponents(String dsn, String dbType) {
        try {
            // Find the query part manually to preserve backslashes in parameter values (Windows paths)
            String baseDsn = dsn;
            String query = null;
            int queryIdx = dsn.indexOf('?');
            if (queryIdx != -1) {
                baseDsn = dsn.substring(0, queryIdx);
                query = dsn.substring(queryIdx + 1);
            }

            // Normalize backslashes ONLY in the authority/path part to avoid URI parsing errors.
            // Query parameters (Windows paths) must stay untouched.
            String basePart = baseDsn.replace("\\", "/");

            // Use a custom approach for authority because URI.getHost() fails with underscores (common in TNS Aliases)
            String scheme = null;
            String userInfo = null;
            String host = null;
            int port = -1;
            String path = null;

            int schemeIdx = basePart.indexOf("://");
            if (schemeIdx != -1) {
                scheme = basePart.substring(0, schemeIdx);
                String authorityAndPath = basePart.substring(schemeIdx + 3);

                int pathIdx = authorityAndPath.indexOf('/');
                String authority;
                if (pathIdx != -1) {
                    authority = authorityAndPath.substring(0, pathIdx);
                    path = authorityAndPath.substring(pathIdx + 1);
                } else {
                    authority = authorityAndPath;
                }

                // Use lastIndexOf to handle passwords containing '@'
                int atIdx = authority.lastIndexOf('@');
                String hostPort;
                if (atIdx != -1) {
                    userInfo = authority.substring(0, atIdx);
                    hostPort = authority.substring(atIdx + 1);
                } else {
                    hostPort = authority;
                }

                int colonIdx = hostPort.lastIndexOf(':');
                if (colonIdx != -1 && colonIdx > hostPort.lastIndexOf(']')) {
                    host = hostPort.substring(0, colonIdx);
                    try {
                        port = Integer.parseInt(hostPort.substring(colonIdx + 1));
                    } catch (NumberFormatException e) {
                        host = hostPort;
                    }
                } else {
                    host = hostPort;
                }
            }

            if (dbType == null || dbType.isEmpty()) {
                dbType = scheme;
            }

            String username = "";
            String password = "";
            if (userInfo != null && userInfo.contains(":")) {
                String[] parts = userInfo.split(":", 2);
                username = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
                password = URLDecoder.decode(parts[1], StandardCharsets.UTF_8);
            }

            return new ParsedDsn(scheme, dbType, username, password, host, port, path, query);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid DSN format: " + dsn, e);
        }
    }

    public static JdbcConnectionInfo parse(String dsn, String dbType) {
        try {
            ParsedDsn parsed = parseComponents(dsn, dbType);

            String jdbcUrl;
            if ("oracle".equalsIgnoreCase(parsed.getDbType())) {
                jdbcUrl = buildOracleJdbcUrl(parsed.getHost(), parsed.getPort(), parsed.getPath(), parsed.getRawQuery());
            } else if ("postgres".equalsIgnoreCase(parsed.getDbType()) || "postgresql".equalsIgnoreCase(parsed.getDbType())) {
                jdbcUrl = buildPostgresJdbcUrl(parsed.getHost(), parsed.getPort(), parsed.getPath());
            } else {
                throw new IllegalArgumentException("Unsupported database type: " + parsed.getDbType());
            }

            return JdbcConnectionInfo.builder()
                    .url(jdbcUrl)
                    .username(parsed.getUsername())
                    .password(parsed.getPassword())
                    .dbType(parsed.getDbType().toLowerCase())
                    .build();

        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid DSN format: " + dsn, e);
        }
    }

    /**
     * Build Oracle JDBC URL.
     *
     * @param host host
     * @param port port
     * @param path path
     * @param query query string
     * @return jdbc url
     */
    public static String buildOracleJdbcUrl(String host, int port, String path, String query) {
        Map<String, String> queryParams = parseQuery(query);
        String sid = queryParams.get("sid");

        // NOTE: We don't append queryString to the JDBC URL here because 
        // we're passing parameters (like TNS_ADMIN) as properties to the DataSource.
        // Appending them with '?' can cause 'Invalid connection string format' in the thin driver.

        if (sid != null && !sid.isEmpty()) {
            // Priority 1: SID Mode
            if (port == -1) port = 1521;
            return String.format("jdbc:oracle:thin:@%s:%d:%s", host, port, sid);
        } else if (port == -1 && (path == null || path.isEmpty())) {
            // Priority 2: TNS Alias Mode
            return String.format("jdbc:oracle:thin:@%s", host);
        } else {
            // Priority 3: Standard Service Mode
            if (port == -1) port = 1521;
            return String.format("jdbc:oracle:thin:@//%s:%d/%s", host, port, path);
        }
    }

    /**
     * Build Postgres JDBC URL.
     *
     * @param host host
     * @param port port
     * @param path path
     * @return jdbc url
     */
    public static String buildPostgresJdbcUrl(String host, int port, String path) {
        if (port == -1) port = 5432;
        return String.format("jdbc:postgresql://%s:%d/%s", host, port, path);
    }

    public static Map<String, String> parseQuery(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isEmpty()) return params;
        
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            if (idx > 0) {
                String rawKey = pair.substring(0, idx);
                String rawValue = pair.substring(idx + 1);
                String key = URLDecoder.decode(rawKey, StandardCharsets.UTF_8);
                String value = URLDecoder.decode(rawValue, StandardCharsets.UTF_8);
                params.put(key, value);
            }
        }
        return params;
    }
}
