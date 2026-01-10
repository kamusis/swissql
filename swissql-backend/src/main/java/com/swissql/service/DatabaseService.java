package com.swissql.service;

import com.swissql.api.ExecuteRequest;
import com.swissql.api.ExecuteResponse;
import com.swissql.driver.DriverManifest;
import com.swissql.driver.DriverRegistry;
import com.swissql.model.SessionInfo;
import com.swissql.util.DsnParser;
import com.swissql.util.JdbcConnectionInfoResolver;
import com.swissql.util.JdbcConnectionInfo;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.File;
import java.io.Reader;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

@Slf4j
@Service
public class DatabaseService {
    private final Map<String, HikariDataSource> dataSources = new ConcurrentHashMap<>();

    private final JdbcConnectionInfoResolver jdbcConnectionInfoResolver;
    private final DriverRegistry driverRegistry;

    /**
     * Create a database service.
     *
     * @param jdbcConnectionInfoResolver resolver for DSN to JDBC connection info
     * @param driverRegistry driver registry
     */
    public DatabaseService(JdbcConnectionInfoResolver jdbcConnectionInfoResolver, DriverRegistry driverRegistry) {
        this.jdbcConnectionInfoResolver = jdbcConnectionInfoResolver;
        this.driverRegistry = driverRegistry;
    }

    private static final int MAX_LOB_CHARS = 100_000;
    private static final int MAX_BLOB_BYTES = 100_000;
    private static final int MAX_STRING_CHARS = 100_000;
    private static final int MAX_NESTED_DEPTH = 3;
    private static final String UNSUPPORTED_PLACEHOLDER = "[unsupported]";

    private static final Pattern POSTGRES_POSITIONAL_PARAMETER_PATTERN = Pattern.compile("\\$\\d+");

    private static String buildMissingTnsAdminMessage(String tnsAdmin) {
        return "TNS_ADMIN directory does not exist or is not a directory: " + tnsAdmin + ". "
                + "If swissql-backend is running in a container, please refer to the user guide for how to load/mount the TNS_ADMIN directory in the container.";
    }

    public ExecuteResponse metaConninfo(SessionInfo session) {
        ExecuteResponse response = new ExecuteResponse();
        response.setType("tabular");

        ExecuteResponse.DataContent data = new ExecuteResponse.DataContent();
        data.setColumns(List.of(
                buildColumn("key", "VARCHAR"),
                buildColumn("value", "VARCHAR")
        ));

        List<Map<String, Object>> rows = new ArrayList<>();
        addConninfoRow(rows, "session_id", session.getSessionId());
        addConninfoRow(rows, "db_type", session.getDbType());
        addConninfoRow(rows, "dsn", session.getDsn());

        if (session.getCreatedAt() != null) {
            addConninfoRow(rows, "created_at", session.getCreatedAt().toString());
        }
        if (session.getLastAccessedAt() != null) {
            addConninfoRow(rows, "last_accessed_at", session.getLastAccessedAt().toString());
        }
        if (session.getExpiresAt() != null) {
            addConninfoRow(rows, "expires_at", session.getExpiresAt().toString());
        }

        String dbType = session.getDbType() != null ? session.getDbType().toLowerCase() : "";
        try {
            DataSource ds = getDataSource(session);
            try (Connection conn = ds.getConnection()) {
                addConninfoRow(rows, "jdbc_url", safeGet(() -> conn.getMetaData().getURL()));
                addConninfoRow(rows, "jdbc_driver", safeGet(() -> conn.getMetaData().getDriverName()));
                addConninfoRow(rows, "jdbc_driver_version", safeGet(() -> conn.getMetaData().getDriverVersion()));
                addConninfoRow(rows, "database_product", safeGet(() -> conn.getMetaData().getDatabaseProductName()));
                addConninfoRow(rows, "database_version", safeGet(() -> conn.getMetaData().getDatabaseProductVersion()));
                addConninfoRow(rows, "jdbc_user", safeGet(() -> conn.getMetaData().getUserName()));

                if ("postgres".equals(dbType) || "postgresql".equals(dbType)) {
                    addConninfoRow(rows, "current_user", singleStringQuery(conn, "SELECT current_user"));
                    addConninfoRow(rows, "current_database", singleStringQuery(conn, "SELECT current_database()"));
                    addConninfoRow(rows, "current_schema", singleStringQuery(conn, "SELECT current_schema()"));
                    addConninfoRow(rows, "server_addr", singleStringQuery(conn, "SELECT inet_server_addr()::text"));
                    addConninfoRow(rows, "server_port", singleStringQuery(conn, "SELECT inet_server_port()::text"));
                    addConninfoRow(rows, "server_version", singleStringQuery(conn, "SELECT version()"));
                }

                if ("oracle".equals(dbType)) {
                    addConninfoRow(rows, "current_user", singleStringQuery(conn, "SELECT USER FROM DUAL"));
                    addConninfoRow(rows, "current_schema", singleStringQuery(conn, "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM DUAL"));
                    addConninfoRow(rows, "db_name", singleStringQuery(conn, "SELECT SYS_CONTEXT('USERENV','DB_NAME') FROM DUAL"));
                    addConninfoRow(rows, "service_name", singleStringQuery(conn, "SELECT SYS_CONTEXT('USERENV','SERVICE_NAME') FROM DUAL"));
                    addConninfoRow(rows, "instance_name", singleStringQuery(conn, "SELECT SYS_CONTEXT('USERENV','INSTANCE_NAME') FROM DUAL"));
                    addConninfoRow(rows, "server_host", singleStringQuery(conn, "SELECT SYS_CONTEXT('USERENV','SERVER_HOST') FROM DUAL"));
                }
            }
        } catch (Exception ignored) {
            // Best effort only
        }

        data.setRows(rows);
        response.setData(data);

        ExecuteResponse.Metadata metadata = new ExecuteResponse.Metadata();
        metadata.setRowsAffected(rows.size());
        metadata.setTruncated(false);
        metadata.setDurationMs(0);
        response.setMetadata(metadata);
        return response;
    }

    public ExecuteResponse metaDescribe(SessionInfo session, String name, String detail) throws SQLException {
        String dbType = session.getDbType() != null ? session.getDbType().toLowerCase() : "";
        String resolvedDetail = detail != null && !detail.isBlank() ? detail.toLowerCase() : "basic";

        ParsedObjectName parsed = ParsedObjectName.parse(name);
        String resolvedSchema = parsed.schema != null && !parsed.schema.isBlank() ? parsed.schema : getDefaultSchema(session);

        if ("oracle".equals(dbType)) {
            String sql;
            if ("full".equals(resolvedDetail)) {
                sql = "SELECT c.COLUMN_NAME AS NAME, c.DATA_TYPE AS TYPE, c.NULLABLE, c.DATA_DEFAULT, "
                        + "cc.COMMENTS AS COLUMN_COMMENT, "
                        + "(SELECT LISTAGG(ac.CONSTRAINT_TYPE || ':' || ac.CONSTRAINT_NAME, ', ') WITHIN GROUP (ORDER BY ac.CONSTRAINT_TYPE, ac.CONSTRAINT_NAME) "
                        + "   FROM ALL_CONS_COLUMNS acc "
                        + "   JOIN ALL_CONSTRAINTS ac ON ac.OWNER = acc.OWNER AND ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME "
                        + "  WHERE acc.OWNER = c.OWNER AND acc.TABLE_NAME = c.TABLE_NAME AND acc.COLUMN_NAME = c.COLUMN_NAME "
                        + ") AS COLUMN_CONSTRAINTS, "
                        + "(SELECT LISTAGG(aic.INDEX_NAME, ', ') WITHIN GROUP (ORDER BY aic.INDEX_NAME) "
                        + "   FROM ALL_IND_COLUMNS aic "
                        + "  WHERE aic.INDEX_OWNER = c.OWNER AND aic.TABLE_NAME = c.TABLE_NAME AND aic.COLUMN_NAME = c.COLUMN_NAME "
                        + ") AS COLUMN_INDEXES "
                        + "FROM ALL_TAB_COLUMNS c "
                        + "LEFT JOIN ALL_COL_COMMENTS cc ON cc.OWNER = c.OWNER AND cc.TABLE_NAME = c.TABLE_NAME AND cc.COLUMN_NAME = c.COLUMN_NAME "
                        + "WHERE c.OWNER = ? AND c.TABLE_NAME = ? "
                        + "ORDER BY c.COLUMN_ID";
            } else {
                sql = "SELECT COLUMN_NAME AS NAME, DATA_TYPE AS TYPE, NULLABLE, DATA_DEFAULT "
                        + "FROM ALL_TAB_COLUMNS "
                        + "WHERE OWNER = ? AND TABLE_NAME = ? "
                        + "ORDER BY COLUMN_ID";
            }

            ExecuteResponse resp = queryTabular(session, sql, List.of(resolvedSchema.toUpperCase(), parsed.object.toUpperCase()), 0);
            if (resp != null && resp.getData() != null && resp.getData().getRows() != null && !resp.getData().getRows().isEmpty()) {
                resp.setSchema(resolvedSchema.toUpperCase());
                return resp;
            }
            ParsedObjectName resolved = resolveOracleSynonym(session, resolvedSchema, parsed.object);
            if (resolved != null && resolved.schema != null && !resolved.schema.isBlank()) {
                resp = queryTabular(session, sql, List.of(resolved.schema.toUpperCase(), resolved.object.toUpperCase()), 0);
                if (resp != null && resp.getData() != null && resp.getData().getRows() != null && !resp.getData().getRows().isEmpty()) {
                    resp.setSchema(resolved.schema.toUpperCase());
                }
            }
            return resp;
        }

        if ("postgres".equals(dbType) || "postgresql".equals(dbType)) {
            String sql;
            if ("full".equals(resolvedDetail)) {
                sql = "SELECT c.column_name AS name, c.data_type AS type, c.is_nullable AS nullable, c.column_default AS data_default, "
                        + "pgd.description AS comment, "
                        + "(SELECT string_agg(tc.constraint_type || ':' || tc.constraint_name, ', ' ORDER BY tc.constraint_type, tc.constraint_name) "
                        + "   FROM information_schema.key_column_usage kcu "
                        + "   JOIN information_schema.table_constraints tc "
                        + "     ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema AND tc.table_name = kcu.table_name "
                        + "  WHERE kcu.table_schema = c.table_schema AND kcu.table_name = c.table_name AND kcu.column_name = c.column_name "
                        + ") AS constraints, "
                        + "(SELECT string_agg(ic.relname, ', ' ORDER BY ic.relname) "
                        + "   FROM pg_catalog.pg_class tc "
                        + "   JOIN pg_catalog.pg_namespace tn ON tn.oid = tc.relnamespace "
                        + "   JOIN pg_catalog.pg_index ix ON ix.indrelid = tc.oid "
                        + "   JOIN pg_catalog.pg_class ic ON ic.oid = ix.indexrelid "
                        + "   JOIN pg_catalog.pg_attribute ia ON ia.attrelid = tc.oid "
                        + "  WHERE tn.nspname = c.table_schema "
                        + "    AND tc.relname = c.table_name "
                        + "    AND ia.attname = c.column_name "
                        + "    AND ia.attnum = ANY(ix.indkey::smallint[]) "
                        + ") AS indexes "
                        + "FROM information_schema.columns c "
                        + "LEFT JOIN pg_catalog.pg_namespace pgn ON pgn.nspname = c.table_schema "
                        + "LEFT JOIN pg_catalog.pg_class pgc ON pgc.relname = c.table_name AND pgc.relnamespace = pgn.oid "
                        + "LEFT JOIN pg_catalog.pg_attribute pga ON pga.attrelid = pgc.oid AND pga.attname = c.column_name "
                        + "LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = pgc.oid AND pgd.objsubid = pga.attnum "
                        + "WHERE c.table_schema = ? AND c.table_name = ? "
                        + "ORDER BY c.ordinal_position";
            } else {
                sql = "SELECT column_name AS name, data_type AS type, is_nullable AS nullable, column_default AS data_default "
                        + "FROM information_schema.columns "
                        + "WHERE table_schema = ? AND table_name = ? "
                        + "ORDER BY ordinal_position";
            }
            ExecuteResponse resp = queryTabular(session, sql, List.of(resolvedSchema.toLowerCase(), parsed.object.toLowerCase()), 0);
            if (resp != null && resp.getData() != null && resp.getData().getRows() != null && !resp.getData().getRows().isEmpty()) {
                resp.setSchema(resolvedSchema.toLowerCase());
                return resp;
            }
            resp = queryTabular(session, sql, List.of("pg_catalog", parsed.object.toLowerCase()), 0);
            if (resp != null && resp.getData() != null && resp.getData().getRows() != null && !resp.getData().getRows().isEmpty()) {
                resp.setSchema("pg_catalog");
                return resp;
            }
            resp = queryTabular(session, sql, List.of("information_schema", parsed.object.toLowerCase()), 0);
            if (resp != null) {
                resp.setSchema("information_schema");
            }
            return resp;
        }

        return buildUnsupportedMetaResponse("meta/describe", dbType);
    }

    private ExecuteResponse.ColumnDefinition buildColumn(String name, String type) {
        ExecuteResponse.ColumnDefinition col = new ExecuteResponse.ColumnDefinition();
        col.setName(name);
        col.setType(type);
        return col;
    }

    private void addConninfoRow(List<Map<String, Object>> rows, String key, String value) {
        rows.add(Map.of("key", key, "value", value != null ? value : ""));
    }

    private String singleStringQuery(Connection conn, String sql) {
        if (conn == null || sql == null || sql.isBlank()) {
            return "";
        }
        try (Statement st = conn.createStatement()) {
            try (ResultSet rs = st.executeQuery(sql)) {
                if (rs.next()) {
                    String v = rs.getString(1);
                    return v != null ? v : "";
                }
                return "";
            }
        } catch (Exception ignored) {
            return "";
        }
    }

    private String safeGet(SqlSupplier supplier) {
        if (supplier == null) {
            return "";
        }
        try {
            String v = supplier.get();
            return v != null ? v : "";
        } catch (Exception ignored) {
            return "";
        }
    }

    @FunctionalInterface
    private interface SqlSupplier {
        String get() throws Exception;
    }

    public ExecuteResponse metaList(SessionInfo session, String kind, String schema) throws SQLException {
        String dbType = session.getDbType() != null ? session.getDbType().toLowerCase() : "";
        String resolvedKind = kind != null && !kind.isBlank() ? kind.toLowerCase() : "table";
        String resolvedSchema = (schema != null && !schema.isBlank()) ? schema : getDefaultSchema(session);

        if ("oracle".equals(dbType)) {
            String sql;
            if ("view".equals(resolvedKind)) {
                sql = "SELECT OWNER AS SCHEMA, VIEW_NAME AS NAME, 'VIEW' AS KIND FROM ALL_VIEWS WHERE OWNER = ? ORDER BY VIEW_NAME";
            } else {
                sql = "SELECT OWNER AS SCHEMA, TABLE_NAME AS NAME, 'TABLE' AS KIND FROM ALL_TABLES WHERE OWNER = ? ORDER BY TABLE_NAME";
            }
            return queryTabular(session, sql, List.of(resolvedSchema.toUpperCase()), 0);
        }

        if ("postgres".equals(dbType) || "postgresql".equals(dbType)) {
            String tableType = "table".equals(resolvedKind) ? "BASE TABLE" : "VIEW";
            String sql = "SELECT table_schema AS schema, table_name AS name, table_type AS kind "
                    + "FROM information_schema.tables "
                    + "WHERE table_schema = ? AND table_type = ? "
                    + "ORDER BY table_name";
            return queryTabular(session, sql, List.of(resolvedSchema.toLowerCase(), tableType), 0);
        }

        return buildUnsupportedMetaResponse("meta/list", dbType);
    }

    public ExecuteResponse metaExplain(SessionInfo session, String sql) throws SQLException {
        return metaExplain(session, sql, false);
    }

    public ExecuteResponse metaExplain(SessionInfo session, String sql, boolean analyze) throws SQLException {
        String dbType = session.getDbType() != null ? session.getDbType().toLowerCase() : "";

        if ("oracle".equals(dbType)) {
            if (analyze) {
                return explainOracleAnalyze(session, sql);
            }
            return explainOracle(session, sql);
        }

        if ("postgres".equals(dbType) || "postgresql".equals(dbType)) {
            if (containsPostgresPositionalParameters(sql)) {
                throw new SQLException(
                        "Cannot EXPLAIN SQL that contains positional parameters like $1 without parameter values. "
                                + "This usually happens when the SQL text is sourced from pg_stat_statements (normalized form). "
                                + "Please run \\\\explain on the original SQL with literals, or manually replace $n placeholders with concrete values before explaining."
                );
            }
            if (analyze) {
                return queryTabular(session, "EXPLAIN (ANALYZE, FORMAT TEXT) " + sql, List.of(), 0);
            }
            return queryTabular(session, "EXPLAIN (FORMAT TEXT) " + sql, List.of(), 0);
        }

        return buildUnsupportedMetaResponse("meta/explain", dbType);
    }

    /**
     * Detects PostgreSQL positional parameters like $1, $2, ... in SQL strings.
     *
     * @param sql SQL text
     * @return true if positional parameters are present
     */
    private boolean containsPostgresPositionalParameters(String sql) {
        if (sql == null || sql.isBlank()) {
            return false;
        }
        return POSTGRES_POSITIONAL_PARAMETER_PATTERN.matcher(sql).find();
    }

    /**
     * Build a stable response for unsupported meta commands on a dbType.
     *
     * @param command meta command
     * @param dbType dbType
     * @return execute response
     */
    private ExecuteResponse buildUnsupportedMetaResponse(String command, String dbType) {
        ExecuteResponse response = new ExecuteResponse();
        response.setType("text");

        ExecuteResponse.DataContent data = new ExecuteResponse.DataContent();
        data.setTextContent("Unsupported meta command: " + command + " for dbType: " + dbType);
        response.setData(data);

        ExecuteResponse.Metadata metadata = new ExecuteResponse.Metadata();
        metadata.setRowsAffected(0);
        metadata.setTruncated(false);
        metadata.setDurationMs(0);
        response.setMetadata(metadata);

        return response;
    }

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

    private ExecuteResponse explainOracleAnalyze(SessionInfo session, String sql) throws SQLException {
        DataSource ds = getDataSource(session);
        long startTime = System.currentTimeMillis();

        try (Connection conn = ds.getConnection()) {
            if (session.getOptions().isReadOnly()) {
                conn.setReadOnly(true);
            }

            try (Statement stmt = conn.createStatement()) {
                try {
                    stmt.execute("ALTER SESSION SET statistics_level=ALL");
                } catch (SQLException ignored) {
                    // Best-effort. Some environments may restrict this.
                }

                boolean hasResultSet = stmt.execute(sql);
                if (hasResultSet) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        while (rs != null && rs.next()) {
                            // Consume result set to ensure runtime stats are collected.
                        }
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(NULL, NULL, 'ALLSTATS LAST'))")) {
                try (ResultSet rs = ps.executeQuery()) {
                    long duration = System.currentTimeMillis() - startTime;
                    ExecuteResponse response = new ExecuteResponse();
                    ExecuteResponse.Metadata metadata = new ExecuteResponse.Metadata();
                    metadata.setDurationMs(duration);
                    response.setMetadata(metadata);
                    response.setType("tabular");
                    processResultSet(rs, response, 0);
                    response.setMetadata(metadata);
                    return response;
                }
            }
        }
    }

    public void testConnection(SessionInfo session) throws SQLException {
        String dsn = session.getDsn();
        JdbcConnectionInfo info = jdbcConnectionInfoResolver.resolve(dsn, session.getDbType());
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
                row.put(rsmd.getColumnName(i), readJsonSafeValue(rs, i));
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

    private Object readJsonSafeValue(ResultSet rs, int columnIndex) throws SQLException {
        try {
            Object v = rs.getObject(columnIndex);
            return sanitizeToJsonSafe(v, 0);
        } catch (Exception ignored) {
            return UNSUPPORTED_PLACEHOLDER;
        }
    }

    private Object sanitizeToJsonSafe(Object v, int depth) throws SQLException {
        if (v == null) {
            return null;
        }
        if (depth > MAX_NESTED_DEPTH) {
            return UNSUPPORTED_PLACEHOLDER;
        }

        Object reflected = tryReadDriverSpecificValue(v);
        if (reflected != null) {
            return truncateString(String.valueOf(reflected));
        }

        if (v instanceof Number || v instanceof Boolean) {
            return v;
        }
        if (v instanceof String s) {
            return truncateString(s);
        }

        if (v instanceof Clob clob) {
            return readClob(clob);
        }
        if (v instanceof Blob blob) {
            return readBlobBase64(blob);
        }
        if (v instanceof SQLXML xml) {
            return truncateString(xml.getString());
        }
        if (v instanceof java.sql.Date || v instanceof java.sql.Time || v instanceof java.sql.Timestamp) {
            return v.toString();
        }
        if (v instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (v instanceof java.util.UUID) {
            return v.toString();
        }

        if (v instanceof Struct struct) {
            Object[] attrs = struct.getAttributes();
            Object[] safe = attrs != null ? attrs : new Object[0];
            List<Object> out = new ArrayList<>(safe.length);
            for (Object attr : safe) {
                out.add(sanitizeToJsonSafe(attr, depth + 1));
            }
            return out;
        }

        if (v instanceof java.sql.Array arr) {
            Object arrayValue = arr.getArray();
            if (arrayValue instanceof Object[] objectArray) {
                List<Object> out = new ArrayList<>(objectArray.length);
                for (Object elem : objectArray) {
                    out.add(sanitizeToJsonSafe(elem, depth + 1));
                }
                return out;
            }
            return truncateString(String.valueOf(arrayValue));
        }

        if (v instanceof Ref ref) {
            return truncateString(ref.getBaseTypeName());
        }

        return truncateString(String.valueOf(v));
    }

    private String truncateString(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() <= MAX_STRING_CHARS) {
            return s;
        }
        return s.substring(0, MAX_STRING_CHARS);
    }

    private Object tryReadDriverSpecificValue(Object v) {
        if (v == null) {
            return null;
        }

        String className = v.getClass().getName();

        // PostgreSQL: JSON/JSONB/custom types may return org.postgresql.util.PGobject
        if ("org.postgresql.util.PGobject".equals(className)) {
            try {
                var m = v.getClass().getMethod("getValue");
                Object value = m.invoke(v);
                return value != null ? value : "";
            } catch (Exception ignored) {
                return null;
            }
        }

        // Oracle: oracle.sql.TIMESTAMP/TIMESTAMPTZ may not be a java.sql.Timestamp instance.
        if (className.startsWith("oracle.sql.TIMESTAMP")) {
            try {
                var m = v.getClass().getMethod("timestampValue");
                Object ts = m.invoke(v);
                return ts != null ? ts.toString() : "";
            } catch (Exception ignored) {
                return null;
            }
        }

        return null;
    }

    private String readClob(Clob clob) throws SQLException {
        long length = clob.length();
        int toRead = (int) Math.min(length, MAX_LOB_CHARS);
        try {
            if (toRead <= 0) {
                return "";
            }
            return clob.getSubString(1, toRead);
        } catch (SQLException ignored) {
            try (Reader reader = clob.getCharacterStream()) {
                if (reader == null) {
                    return "";
                }
                char[] buf = new char[Math.min(MAX_LOB_CHARS, 8192)];
                StringBuilder sb = new StringBuilder();
                int n;
                while (sb.length() < MAX_LOB_CHARS && (n = reader.read(buf, 0, Math.min(buf.length, MAX_LOB_CHARS - sb.length()))) > 0) {
                    sb.append(buf, 0, n);
                }
                return sb.toString();
            } catch (Exception e) {
                return "";
            }
        }
    }

    private String readBlobBase64(Blob blob) throws SQLException {
        long length = blob.length();
        int toRead = (int) Math.min(length, MAX_BLOB_BYTES);
        if (toRead <= 0) {
            return "";
        }
        byte[] bytes = blob.getBytes(1, toRead);
        return Base64.getEncoder().encodeToString(bytes);
    }

    private DataSource getDataSource(SessionInfo session) {
        return dataSources.computeIfAbsent(session.getSessionId(), sid -> {
            HikariConfig config = buildHikariConfig(session, "Pool-" + sid, 5, 1);
            config.setIdleTimeout(60000);
            return new HikariDataSource(config);
        });
    }

    public Connection getConnection(SessionInfo session) throws SQLException {
        return getDataSource(session).getConnection();
    }

    private ExecuteResponse queryTabular(SessionInfo session, String sql, List<Object> params, int limit) throws SQLException {
        DataSource ds = getDataSource(session);
        long startTime = System.currentTimeMillis();

        try (Connection conn = ds.getConnection()) {
            if (session.getOptions().isReadOnly()) {
                conn.setReadOnly(true);
            }

            // For parameter-less queries, prefer Statement over PreparedStatement.
            // This avoids PostgreSQL JDBC extended-protocol edge cases (SQLSTATE 08P01)
            // that can mark pooled connections as broken and exhaust Hikari pool.
            if (params == null || params.isEmpty()) {
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        long duration = System.currentTimeMillis() - startTime;
                        ExecuteResponse response = new ExecuteResponse();
                        ExecuteResponse.Metadata metadata = new ExecuteResponse.Metadata();
                        metadata.setDurationMs(duration);
                        response.setMetadata(metadata);
                        response.setType("tabular");
                        processResultSet(rs, response, limit);
                        response.setMetadata(metadata);
                        return response;
                    }
                }
            }

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    stmt.setObject(i + 1, params.get(i));
                }

                try (ResultSet rs = stmt.executeQuery()) {
                    long duration = System.currentTimeMillis() - startTime;
                    ExecuteResponse response = new ExecuteResponse();
                    ExecuteResponse.Metadata metadata = new ExecuteResponse.Metadata();
                    metadata.setDurationMs(duration);
                    response.setMetadata(metadata);
                    response.setType("tabular");
                    processResultSet(rs, response, limit);
                    response.setMetadata(metadata);
                    return response;
                }
            }
        }
    }

    private ExecuteResponse explainOracle(SessionInfo session, String sql) throws SQLException {
        DataSource ds = getDataSource(session);
        long startTime = System.currentTimeMillis();

        try (Connection conn = ds.getConnection()) {
            if (session.getOptions().isReadOnly()) {
                conn.setReadOnly(true);
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("EXPLAIN PLAN FOR " + sql);
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY())")) {
                try (ResultSet rs = ps.executeQuery()) {
                    long duration = System.currentTimeMillis() - startTime;
                    ExecuteResponse response = new ExecuteResponse();
                    ExecuteResponse.Metadata metadata = new ExecuteResponse.Metadata();
                    metadata.setDurationMs(duration);
                    response.setMetadata(metadata);
                    response.setType("tabular");
                    processResultSet(rs, response, 0);
                    response.setMetadata(metadata);
                    return response;
                }
            }
        }
    }

    private String getDefaultSchema(SessionInfo session) {
        try {
            DataSource ds = getDataSource(session);
            try (Connection conn = ds.getConnection()) {
                String dbType = session.getDbType() != null ? session.getDbType().toLowerCase() : "";

                if ("postgres".equals(dbType) || "postgresql".equals(dbType)) {
                    try (Statement st = conn.createStatement()) {
                        try (ResultSet rs = st.executeQuery("SELECT current_schema()")) {
                            if (rs.next()) {
                                String schema = rs.getString(1);
                                if (schema != null) {
                                    return schema;
                                }
                            }
                        }
                    }
                }

                String username = conn.getMetaData().getUserName();
                if (username == null) {
                    return "";
                }
                return username;
            }
        } catch (Exception e) {
            return "";
        }
    }

    private ParsedObjectName resolveOracleSynonym(SessionInfo session, String schema, String objectName) {
        if (objectName == null || objectName.isBlank()) {
            return null;
        }
        String resolvedSchema = schema != null && !schema.isBlank() ? schema : getDefaultSchema(session);

        // Prefer schema synonym, then PUBLIC synonym
        String sql = "SELECT TABLE_OWNER, TABLE_NAME "
                + "FROM ALL_SYNONYMS "
                + "WHERE SYNONYM_NAME = ? AND (OWNER = ? OR OWNER = 'PUBLIC') "
                + "ORDER BY CASE WHEN OWNER = ? THEN 0 ELSE 1 END";

        try {
            DataSource ds = getDataSource(session);
            try (Connection conn = ds.getConnection()) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, objectName.toUpperCase());
                    ps.setString(2, resolvedSchema.toUpperCase());
                    ps.setString(3, resolvedSchema.toUpperCase());

                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            String owner = rs.getString(1);
                            String table = rs.getString(2);
                            if (owner != null && table != null) {
                                return new ParsedObjectName(owner, table);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            return null;
        }

        return null;
    }

    private static class ParsedObjectName {
        private final String schema;
        private final String object;

        private ParsedObjectName(String schema, String object) {
            this.schema = schema;
            this.object = object;
        }

        private static ParsedObjectName parse(String name) {
            if (name == null) {
                return new ParsedObjectName("", "");
            }
            String trimmed = name.trim();
            int dot = trimmed.indexOf('.');
            if (dot > 0 && dot < trimmed.length() - 1) {
                return new ParsedObjectName(trimmed.substring(0, dot), trimmed.substring(dot + 1));
            }
            return new ParsedObjectName("", trimmed);
        }
    }

    private HikariConfig buildHikariConfig(SessionInfo session, String poolName, int maximumPoolSize, int minimumIdle) {
        String dsn = session.getDsn();
        String query = extractQuery(dsn);
        Map<String, String> queryParams = DsnParser.parseQuery(query);

        JdbcConnectionInfo info = jdbcConnectionInfoResolver.resolve(dsn, session.getDbType());
        HikariConfig config = new HikariConfig();
        config.setExceptionOverrideClassName("com.swissql.service.HikariSqlExceptionOverride");
        config.setJdbcUrl(info.getUrl());
        config.setUsername(info.getUsername());
        config.setPassword(info.getPassword());

        if ("oracle".equalsIgnoreCase(info.getDbType())) {
            config.setDriverClassName("oracle.jdbc.OracleDriver");

            if (queryParams.containsKey("TNS_ADMIN")) {
                String tnsAdmin = queryParams.get("TNS_ADMIN").replace("\\", "/");
                File tnsDir = new File(tnsAdmin);
                if (!tnsDir.exists() || !tnsDir.isDirectory()) {
                    throw new IllegalArgumentException(buildMissingTnsAdminMessage(tnsAdmin));
                }
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
        } else {
            DriverRegistry.Entry entry = driverRegistry.find(info.getDbType()).orElse(null);
            DriverManifest manifest = entry != null ? entry.getManifest() : null;
            if (manifest == null || manifest.getDriverClass() == null || manifest.getDriverClass().isBlank()) {
                throw new IllegalArgumentException("Missing driver.json manifest or driverClass for dbType: " + info.getDbType());
            }
            // Do NOT set driverClassName for dynamically loaded drivers.
            // Hikari will attempt to load the class via the application/TCCL classloader, which will fail
            // for URLClassLoader-isolated drivers. Instead, rely on DriverManager, where DriverShim has
            // been registered during auto-load.
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

    public ExecuteResponse metaCompletions(SessionInfo session, String kind, String schema, String table, String prefix) throws SQLException {
        String dbType = session.getDbType() != null ? session.getDbType().toLowerCase() : "";
        String resolvedKind = kind != null && !kind.isBlank() ? kind.toLowerCase() : "tables";
        String resolvedSchema = schema != null && !schema.isBlank() ? schema : getDefaultSchema(session);
        String resolvedTable = table != null && !table.isBlank() ? table : "";
        String resolvedPrefix = prefix != null && !prefix.isBlank() ? prefix : "";

        ExecuteResponse response = new ExecuteResponse();
        response.setType("tabular");

        ExecuteResponse.DataContent data = new ExecuteResponse.DataContent();
        data.setColumns(List.of(buildColumn("name", "VARCHAR")));

        List<Map<String, Object>> rows = new ArrayList<>();

        DataSource ds = getDataSource(session);
        try (Connection conn = ds.getConnection()) {
            String sql;
            boolean isOracle = "oracle".equals(dbType);
            boolean isPostgres = "postgres".equals(dbType) || "postgresql".equals(dbType);

            if (isOracle) {
                sql = buildOracleCompletionSql(resolvedKind, resolvedSchema, resolvedTable, resolvedPrefix);
            } else if (isPostgres) {
                sql = buildPostgresCompletionSql(resolvedKind, resolvedSchema, resolvedTable, resolvedPrefix);
            } else {
                response.setData(data);
                response.setMetadata(new ExecuteResponse.Metadata());
                return response;
            }

            if (sql != null && !sql.isBlank()) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    int paramIndex = 1;
                    if (!resolvedSchema.isBlank()) {
                        ps.setString(paramIndex++, isOracle ? resolvedSchema.toUpperCase() : resolvedSchema);
                    }
                    if (!resolvedTable.isBlank()) {
                        ps.setString(paramIndex++, isOracle ? resolvedTable.toUpperCase() : resolvedTable);
                    }
                    if (!resolvedPrefix.isBlank()) {
                        ps.setString(paramIndex++, isOracle ? resolvedPrefix.toUpperCase() + "%" : resolvedPrefix + "%");
                    }

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String name = rs.getString(1);
                            if (name != null) {
                                Map<String, Object> row = new HashMap<>();
                                row.put("name", name);
                                rows.add(row);
                            }
                        }
                    }
                }
            }
        }

        data.setRows(rows);
        response.setData(data);

        ExecuteResponse.Metadata metadata = new ExecuteResponse.Metadata();
        metadata.setRowsAffected(rows.size());
        metadata.setTruncated(false);
        metadata.setDurationMs(0);
        response.setMetadata(metadata);

        return response;
    }

    private String buildOracleCompletionSql(String kind, String schema, String table, String prefix) {
        switch (kind) {
            case "tables":
                if (!schema.isBlank()) {
                    return !prefix.isBlank()
                            ? "SELECT table_name AS name FROM all_tables WHERE owner = ? AND UPPER(table_name) LIKE ? ORDER BY table_name"
                            : "SELECT table_name AS name FROM all_tables WHERE owner = ? ORDER BY table_name";
                } else {
                    return !prefix.isBlank()
                            ? "SELECT table_name AS name FROM user_tables WHERE UPPER(table_name) LIKE ? ORDER BY table_name"
                            : "SELECT table_name AS name FROM user_tables ORDER BY table_name";
                }
            case "views":
                if (!schema.isBlank()) {
                    return !prefix.isBlank()
                            ? "SELECT view_name AS name FROM all_views WHERE owner = ? AND UPPER(view_name) LIKE ? ORDER BY view_name"
                            : "SELECT view_name AS name FROM all_views WHERE owner = ? ORDER BY view_name";
                } else {
                    return !prefix.isBlank()
                            ? "SELECT view_name AS name FROM user_views WHERE UPPER(view_name) LIKE ? ORDER BY view_name"
                            : "SELECT view_name AS name FROM user_views ORDER BY view_name";
                }
            case "schemas":
                return !prefix.isBlank()
                        ? "SELECT username AS name FROM all_users WHERE UPPER(username) LIKE ? ORDER BY username"
                        : "SELECT username AS name FROM all_users ORDER BY username";
            case "columns":
                if (!table.isBlank()) {
                    if (!schema.isBlank()) {
                        return !prefix.isBlank()
                                ? "SELECT column_name AS name FROM all_tab_columns WHERE owner = ? AND table_name = ? AND UPPER(column_name) LIKE ? ORDER BY column_id"
                                : "SELECT column_name AS name FROM all_tab_columns WHERE owner = ? AND table_name = ? ORDER BY column_id";
                    } else {
                        return !prefix.isBlank()
                                ? "SELECT column_name AS name FROM user_tab_columns WHERE table_name = ? AND UPPER(column_name) LIKE ? ORDER BY column_id"
                                : "SELECT column_name AS name FROM user_tab_columns WHERE table_name = ? ORDER BY column_id";
                    }
                }
                return null;
            default:
                return null;
        }
    }

    private String buildPostgresCompletionSql(String kind, String schema, String table, String prefix) {
        switch (kind) {
            case "tables":
                if (!schema.isBlank()) {
                    return !prefix.isBlank()
                            ? "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' AND table_name ILIKE ? ORDER BY table_name"
                            : "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE' ORDER BY table_name";
                } else {
                    return !prefix.isBlank()
                            ? "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' AND table_name ILIKE ? ORDER BY table_name"
                            : "SELECT table_name AS name FROM information_schema.tables WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' ORDER BY table_name";
                }
            case "views":
                if (!schema.isBlank()) {
                    return !prefix.isBlank()
                            ? "SELECT table_name AS name FROM information_schema.views WHERE table_schema = ? AND table_name ILIKE ? ORDER BY table_name"
                            : "SELECT table_name AS name FROM information_schema.views WHERE table_schema = ? ORDER BY table_name";
                } else {
                    return !prefix.isBlank()
                            ? "SELECT table_name AS name FROM information_schema.views WHERE table_schema = current_schema() AND table_name ILIKE ? ORDER BY table_name"
                            : "SELECT table_name AS name FROM information_schema.views WHERE table_schema = current_schema() ORDER BY table_name";
                }
            case "schemas":
                return !prefix.isBlank()
                        ? "SELECT schema_name AS name FROM information_schema.schemata WHERE schema_name ILIKE ? ORDER BY schema_name"
                        : "SELECT schema_name AS name FROM information_schema.schemata ORDER BY schema_name";
            case "columns":
                if (!table.isBlank()) {
                    if (!schema.isBlank()) {
                        return !prefix.isBlank()
                                ? "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name ILIKE ? ORDER BY ordinal_position"
                                : "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position";
                    } else {
                        return !prefix.isBlank()
                                ? "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? AND column_name ILIKE ? ORDER BY ordinal_position"
                                : "SELECT column_name AS name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = ? ORDER BY ordinal_position";
                    }
                }
                return null;
            default:
                return null;
        }
    }
}
