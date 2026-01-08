package com.swissql.api;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Response for {@code GET /v1/meta/drivers}.
 */
@Data
public class DriversResponse {

    /**
     * List of driver entries.
     */
    private List<DriverEntry> drivers = new ArrayList<>();

    /**
     * One driver entry per dbType.
     */
    @Data
    public static class DriverEntry {
        private String dbType;
        private String source;

        /**
         * Configured driver class from driver.json (directory source only).
         */
        private String driverClass;

        /**
         * Discovered driver classes from JARs (best-effort).
         */
        private List<String> driverClasses = new ArrayList<>();

        /**
         * Best-effort list of jar paths.
         */
        private List<String> jarPaths = new ArrayList<>();

        /**
         * Configured JDBC URL template (directory source only).
         */
        private String jdbcUrlTemplate;

        /**
         * Configured default port (directory source only).
         */
        private Integer defaultPort;
    }
}
