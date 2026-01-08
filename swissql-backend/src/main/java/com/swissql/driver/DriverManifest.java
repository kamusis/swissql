package com.swissql.driver;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the required {@code driver.json} manifest located under {@code /jdbc_drivers/<dbType>/driver.json}.
 *
 * The manifest is the authoritative source of:
 * - {@code dbType} (must match the directory name)
 * - {@code driverClass} (the JDBC driver class to register)
 * - {@code jdbcUrlTemplate} (used to build JDBC URL from DSN components)
 * - {@code defaultPort} (optional)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DriverManifest {
    @JsonProperty("dbType")
    private String dbType;

    @JsonProperty("driverClass")
    private String driverClass;

    @JsonProperty("jdbcUrlTemplate")
    private String jdbcUrlTemplate;

    @JsonProperty("defaultPort")
    private Integer defaultPort;

    /**
     * Get dbType.
     *
     * @return dbType
     */
    public String getDbType() {
        return dbType;
    }

    /**
     * Set dbType.
     *
     * @param dbType dbType
     */
    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    /**
     * Get driver class.
     *
     * @return driver class
     */
    public String getDriverClass() {
        return driverClass;
    }

    /**
     * Set driver class.
     *
     * @param driverClass driver class
     */
    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    /**
     * Get JDBC URL template.
     *
     * @return JDBC URL template
     */
    public String getJdbcUrlTemplate() {
        return jdbcUrlTemplate;
    }

    /**
     * Set JDBC URL template.
     *
     * @param jdbcUrlTemplate JDBC URL template
     */
    public void setJdbcUrlTemplate(String jdbcUrlTemplate) {
        this.jdbcUrlTemplate = jdbcUrlTemplate;
    }

    /**
     * Get default port.
     *
     * @return default port
     */
    public Integer getDefaultPort() {
        return defaultPort;
    }

    /**
     * Set default port.
     *
     * @param defaultPort default port
     */
    public void setDefaultPort(Integer defaultPort) {
        this.defaultPort = defaultPort;
    }
}
