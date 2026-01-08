package com.swissql.service;

import com.zaxxer.hikari.SQLExceptionOverride;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * HikariCP SQL exception override to reduce log noise and avoid evicting connections
 * for expected, non-fatal SQL errors.
 *
 * Some JDBC drivers may report feature/statement unsupported errors using SQLSTATE 0A000.
 * These are not connection-fatal errors and should not mark a pooled connection as broken.
 */
public class HikariSqlExceptionOverride implements SQLExceptionOverride {

    /**
     * Decide whether Hikari should evict a connection based on the exception.
     *
     * @param sqlException SQL exception
     * @return override decision
     */
    @java.lang.Override
    public SQLExceptionOverride.Override adjudicate(SQLException sqlException) {
        if (sqlException == null) {
            return Override.CONTINUE_EVICT;
        }

        if (sqlException instanceof SQLFeatureNotSupportedException) {
            return Override.DO_NOT_EVICT;
        }

        String sqlState = sqlException.getSQLState();
        if (sqlState != null && sqlState.startsWith("0A")) {
            // 0A000: feature not supported
            return Override.DO_NOT_EVICT;
        }

        return Override.CONTINUE_EVICT;
    }
}
