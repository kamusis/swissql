package com.swissql.util;

import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Utility to convert JDBC driver values into JSON-safe primitives.
 *
 * <p>This is used across multiple API paths (execute_sql, collectors, samplers) to avoid
 * driver-specific objects leaking into JSON serialization.
 */
public final class JdbcJsonSafe {
    private static final int MAX_LOB_CHARS = 100_000;
    private static final int MAX_BLOB_BYTES = 100_000;
    private static final int MAX_STRING_CHARS = 100_000;
    private static final int MAX_NESTED_DEPTH = 3;
    private static final String UNSUPPORTED_PLACEHOLDER = "[unsupported]";

    private JdbcJsonSafe() {
    }

    /**
     * Reads a JDBC column value and returns a JSON-safe equivalent.
     *
     * @param rs result set
     * @param columnIndex 1-based column index
     * @return json-safe value
     * @throws SQLException on JDBC errors
     */
    public static Object readJsonSafeValue(ResultSet rs, int columnIndex) throws SQLException {
        try {
            Object v = rs.getObject(columnIndex);
            return sanitizeToJsonSafe(v, 0);
        } catch (Exception ignored) {
            return UNSUPPORTED_PLACEHOLDER;
        }
    }

    /**
     * Converts an arbitrary JDBC object into a JSON-safe primitive or structure.
     *
     * @param v value to convert
     * @return json-safe value
     * @throws SQLException on JDBC errors
     */
    public static Object toJsonSafe(Object v) throws SQLException {
        try {
            return sanitizeToJsonSafe(v, 0);
        } catch (Exception ignored) {
            return UNSUPPORTED_PLACEHOLDER;
        }
    }

    private static Object sanitizeToJsonSafe(Object v, int depth) throws SQLException {
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

    private static String truncateString(String s) {
        if (s == null) {
            return null;
        }
        if (s.length() <= MAX_STRING_CHARS) {
            return s;
        }
        return s.substring(0, MAX_STRING_CHARS);
    }

    private static Object tryReadDriverSpecificValue(Object v) {
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

    private static String readClob(Clob clob) throws SQLException {
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

    private static String readBlobBase64(Blob blob) throws SQLException {
        long length = blob.length();
        int toRead = (int) Math.min(length, MAX_BLOB_BYTES);
        if (toRead <= 0) {
            return "";
        }
        byte[] bytes = blob.getBytes(1, toRead);
        return Base64.getEncoder().encodeToString(bytes);
    }
}
