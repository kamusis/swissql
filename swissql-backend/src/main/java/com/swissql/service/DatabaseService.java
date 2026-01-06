package com.swissql.service;

import com.swissql.api.ExecuteRequest;
import com.swissql.api.ExecuteResponse;
import com.swissql.model.SessionInfo;
import com.swissql.util.DsnParser;
import com.swissql.util.JdbcConnectionInfo;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class DatabaseService {
    private final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    public void initializeSession(SessionInfo session) throws SQLException {
        String sessionId = session.getSessionId();
        if (sessionId == null || sessionId.isBlank()) {
            throw new SQLException("Session ID is required to initialize a session DataSource");
        }

        if (dataSources.containsKey(sessionId)) {
            return;
        }

        HikariConfig config = buildHikariConfig(session, "Pool-" + sessionId, 5, 1);
        HikariDataSource ds = new HikariDataSource(config);
        try {
            try (Connection conn = ds.getConnection()) {
                if (!conn.isValid(5)) {
                    throw new SQLException("Connection is not valid");
                }
            }

            HikariDataSource existing = dataSources.putIfAbsent(sessionId, ds);
            if (existing != null) {
                ds.close();
            }
        } catch (SQLException e) {
            ds.close();
            throw e;
        }
    }

    public void testConnection(SessionInfo session) throws SQLException {
        // 1. Pre-parse to get TNS_ADMIN and set global system properties immediately
        String dsn = session.getDsn();
        String query = extractQuery(dsn);
        Map<String, String> queryParams = DsnParser.parseQuery(query);
        
        if (queryParams.containsKey("TNS_ADMIN")) {
            String tnsAdmin = queryParams.get("TNS_ADMIN").replace("\\", "/");
            File tnsDir = new File(tnsAdmin);
            if (tnsDir.exists() && tnsDir.isDirectory()) {
                String walletLocation = "(SOURCE=(METHOD=file)(METHOD_DATA=(DIRECTORY=" + tnsAdmin + ")))";
                System.setProperty("oracle.net.tns_admin", tnsAdmin);
                System.setProperty("tns.admin", tnsAdmin);
                System.setProperty("oracle.net.wallet_location", walletLocation);
                log.info("Global TNS_ADMIN and Wallet properties set: {}", tnsAdmin);
            }
        }

        JdbcConnectionInfo info = DsnParser.parse(dsn, session.getDbType());
        log.info("Testing connection for DSN: {} (Type: {})", maskDsn(dsn), info.getDbType());

        HikariConfig config = buildHikariConfig(session, "Test-Connection-" + System.currentTimeMillis(), 1, 0);

        try (HikariDataSource ds = new HikariDataSource(config)) {
            try (Connection conn = ds.getConnection()) {
                if (!conn.isValid(5)) {
                    throw new SQLException("Connection is not valid");
                }
                log.info("Test connection successful");
            }
        } catch (SQLException e) {
            log.error("Test connection failed: {} (SQLState: {}, Error Code: {})", e.getMessage(), e.getSQLState(), e.getErrorCode());
            throw e;
        }
    }

    private String maskDsn(String dsn) {
        if (dsn == null) return null;
        return dsn.replaceAll(":[^@:]+@", ":****@");
    }

    public ExecuteResponse execute(SessionInfo session, ExecuteRequest request) throws SQLException {
        DataSource ds = getDataSource(session);
        long startTime = System.currentTimeMillis();
        
        try (Connection conn = ds.getConnection()) {
            // Enforce read-only if configured
            if (session.getOptions().isReadOnly()) {
                conn.setReadOnly(true);
            }
            
            try (Statement stmt = conn.createStatement()) {
                int queryTimeoutMs = request.getOptions().getQueryTimeoutMs();
                if (queryTimeoutMs > 0) {
                    stmt.setQueryTimeout(queryTimeoutMs / 1000);
                }
                stmt.setFetchSize(request.getOptions().getFetchSize());
                
                boolean isResultSet = stmt.execute(request.getSql());
                long duration = System.currentTimeMillis() - startTime;
                
                ExecuteResponse response = new ExecuteResponse();
                ExecuteResponse.Metadata metadata = new ExecuteResponse.Metadata();
                metadata.setDurationMs((int) duration);
                response.setMetadata(metadata); // Set metadata immediately to avoid NPE in processResultSet
                
                if (isResultSet) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        response.setType("tabular");
                        processResultSet(rs, response, request.getOptions().getLimit());
                    }
                } else {
                    response.setType("text");
                    int updateCount = stmt.getUpdateCount();
                    metadata.setRowsAffected(updateCount);
                    ExecuteResponse.DataContent data = new ExecuteResponse.DataContent();
                    data.setTextContent("Query executed successfully. Rows affected: " + updateCount);
                    response.setData(data);
                }
                
                response.setMetadata(metadata);
                return response;
            }
        }
    }

    private void processResultSet(ResultSet rs, ExecuteResponse response, int limit) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        
        List<ExecuteResponse.ColumnDefinition> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            ExecuteResponse.ColumnDefinition col = new ExecuteResponse.ColumnDefinition();
            col.setName(rsmd.getColumnName(i));
            col.setType(rsmd.getColumnTypeName(i));
            columns.add(col);
        }
        
        List<Map<String, Object>> rows = new ArrayList<>();
        int count = 0;
        boolean truncated = false;
        while (rs.next()) {
            if (limit > 0 && count >= limit) {
                truncated = true;
                break;
            }
            
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                row.put(rsmd.getColumnName(i), rs.getObject(i));
            }
            rows.add(row);
            count++;
        }
        
        ExecuteResponse.DataContent data = new ExecuteResponse.DataContent();
        data.setColumns(columns);
        data.setRows(rows);
        response.setData(data);
        
        if (response.getMetadata() == null) {
            response.setMetadata(new ExecuteResponse.Metadata());
        }
        response.getMetadata().setRowsAffected(count);
        response.getMetadata().setTruncated(truncated);
    }

    private DataSource getDataSource(SessionInfo session) {
        return dataSources.computeIfAbsent(session.getSessionId(), sid -> {
            HikariConfig config = buildHikariConfig(session, "Pool-" + sid, 5, 1);
            config.setIdleTimeout(60000);
            return new HikariDataSource(config);
        });
    }

    private HikariConfig buildHikariConfig(SessionInfo session, String poolName, int maximumPoolSize, int minimumIdle) {
        String dsn = session.getDsn();
        String query = extractQuery(dsn);
        Map<String, String> queryParams = DsnParser.parseQuery(query);

        if (queryParams.containsKey("TNS_ADMIN")) {
            String tnsAdmin = queryParams.get("TNS_ADMIN").replace("\\", "/");
            File tnsDir = new File(tnsAdmin);
            if (tnsDir.exists() && tnsDir.isDirectory()) {
                String walletLocation = "(SOURCE=(METHOD=file)(METHOD_DATA=(DIRECTORY=" + tnsAdmin + ")))";
                System.setProperty("oracle.net.tns_admin", tnsAdmin);
                System.setProperty("tns.admin", tnsAdmin);
                System.setProperty("oracle.net.wallet_location", walletLocation);
            }
        }

        JdbcConnectionInfo info = DsnParser.parse(dsn, session.getDbType());
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(info.getUrl());
        config.setUsername(info.getUsername());
        config.setPassword(info.getPassword());

        if ("oracle".equalsIgnoreCase(info.getDbType())) {
            config.setDriverClassName("oracle.jdbc.OracleDriver");

            if (queryParams.containsKey("TNS_ADMIN")) {
                String tnsAdmin = queryParams.get("TNS_ADMIN").replace("\\", "/");
                String walletLocation = "(SOURCE=(METHOD=file)(METHOD_DATA=(DIRECTORY=" + tnsAdmin + ")))";
                config.addDataSourceProperty("oracle.net.tns_admin", tnsAdmin);
                config.addDataSourceProperty("oracle.net.wallet_location", walletLocation);
                config.addDataSourceProperty("oracle.net.ssl_server_dn_match", "true");
            }
            config.addDataSourceProperty("v$session.program", "swissql");
            config.addDataSourceProperty("v$session.terminal", System.getProperty("user.name", "swissql-client"));

            config.addDataSourceProperty("oracle.jdbc.fanEnabled", "false");
            config.addDataSourceProperty("oracle.jdbc.selfTuning", "false");
        } else if ("postgres".equalsIgnoreCase(info.getDbType())) {
            config.setDriverClassName("org.postgresql.Driver");
            // Ensure pg_stat_activity.application_name is set.
            // For PostgreSQL JDBC, ApplicationName is mapped to the startup parameter application_name.
            config.addDataSourceProperty("ApplicationName", "swissql");
        }

        config.setConnectionTimeout(session.getOptions().getConnectionTimeoutMs());
        config.setMaximumPoolSize(maximumPoolSize);
        config.setMinimumIdle(minimumIdle);
        config.setPoolName(poolName);
        return config;
    }

    private String extractQuery(String url) {
        int idx = url.indexOf('?');
        return idx != -1 ? url.substring(idx + 1) : null;
    }

    public void closeSession(String sessionId) {
        HikariDataSource ds = dataSources.remove(sessionId);
        if (ds != null) {
            ds.close();
        }
    }
}
