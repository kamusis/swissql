package com.swissql.util;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class DsnParser {

    public static JdbcConnectionInfo parse(String dsn, String dbType) {
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
            
            // Normalize dbType if not provided or to match scheme
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

            String jdbcUrl;
            if ("oracle".equalsIgnoreCase(dbType)) {
                jdbcUrl = buildOracleJdbcUrl(host, port, path, query);
            } else if ("postgres".equalsIgnoreCase(dbType) || "postgresql".equalsIgnoreCase(dbType)) {
                jdbcUrl = buildPostgresJdbcUrl(host, port, path);
            } else {
                throw new IllegalArgumentException("Unsupported database type: " + dbType);
            }

            return JdbcConnectionInfo.builder()
                    .url(jdbcUrl)
                    .username(username)
                    .password(password)
                    .dbType(dbType.toLowerCase())
                    .build();

        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid DSN format: " + dsn, e);
        }
    }

    private static String buildOracleJdbcUrl(String host, int port, String path, String query) {
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

    private static String buildPostgresJdbcUrl(String host, int port, String path) {
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
                params.put(pair.substring(0, idx), pair.substring(idx + 1));
            }
        }
        return params;
    }
}
