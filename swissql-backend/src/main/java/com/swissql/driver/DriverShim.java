package com.swissql.driver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/**
 * JDBC {@link Driver} wrapper to avoid classloader issues when registering dynamically loaded drivers.
 *
 * The {@link java.sql.DriverManager} keeps strong references to registered drivers, and in mixed
 * classloader environments the concrete driver may not be visible to the application classloader.
 * This shim delegates all operations to the actual driver instance.
 */
public class DriverShim implements Driver {
    private final Driver delegate;

    /**
     * Create a driver shim.
     *
     * @param delegate actual driver
     */
    public DriverShim(Driver delegate) {
        this.delegate = delegate;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return delegate.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return delegate.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return delegate.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return delegate.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return delegate.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return delegate.jdbcCompliant();
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        try {
            return delegate.getParentLogger();
        } catch (Exception e) {
            return java.util.logging.Logger.getLogger("global");
        }
    }
}
