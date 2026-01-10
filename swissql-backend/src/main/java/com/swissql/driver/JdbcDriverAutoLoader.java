package com.swissql.driver;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.DriverManager;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Scans {@code /jdbc_drivers/<dbType>/} directories, reads {@code driver.json}, loads JARs,
 * and registers JDBC drivers at runtime.
 */
@Service
public class JdbcDriverAutoLoader {

    private static final Logger log = LoggerFactory.getLogger(JdbcDriverAutoLoader.class);

    private static final String DEFAULT_DIR = "/jdbc_drivers";

    private static final Set<String> BUILTIN_DB_TYPES = Set.of("oracle", "postgres");

    private final Environment environment;
    private final ObjectMapper objectMapper;
    private final DriverRegistry driverRegistry;

    /**
     * Create a driver auto-loader.
     *
     * @param environment Spring environment
     * @param objectMapper Jackson object mapper
     * @param driverRegistry registry
     */
    public JdbcDriverAutoLoader(Environment environment, ObjectMapper objectMapper, DriverRegistry driverRegistry) {
        this.environment = environment;
        this.objectMapper = objectMapper;
        this.driverRegistry = driverRegistry;
    }

    /**
     * Load built-in and directory-provided drivers on startup.
     */
    @PostConstruct
    public void init() {
        driverRegistry.registerBuiltin("oracle", "oracle.jdbc.OracleDriver");
        driverRegistry.registerBuiltin("postgres", "org.postgresql.Driver");

        boolean enabled = getBool("swissql.jdbc-drivers.auto-load.enabled", "SWISSQL_JDBC_DRIVERS_AUTO_LOAD_ENABLED", true);
        if (!enabled) {
            log.info("JDBC driver auto-load is DISABLED");
            return;
        }

        String dir = getTrimmed("swissql.jdbc-drivers.auto-load.dir", "SWISSQL_JDBC_DRIVERS_AUTO_LOAD_DIR");
        if (dir == null || dir.isBlank()) {
            dir = DEFAULT_DIR;
        }

        ReloadResult result = reloadInternal(Paths.get(dir));
        log.info(
                "JDBC driver auto-load completed (dir={}, db_types_scanned={}, driver_classes_registered={}, warnings={})",
                dir,
                result.getDbTypesScanned(),
                result.getDriverClassesRegistered(),
                result.getWarnings().size()
        );
        for (String w : result.getWarnings()) {
            log.warn("JDBC driver auto-load warning: {}", w);
        }
    }

    /**
     * Rescan and register drivers.
     *
     * @return reload result
     */
    public synchronized ReloadResult reload() {
        String dir = getTrimmed("swissql.jdbc-drivers.auto-load.dir", "SWISSQL_JDBC_DRIVERS_AUTO_LOAD_DIR");
        if (dir == null || dir.isBlank()) {
            dir = DEFAULT_DIR;
        }
        return reloadInternal(Paths.get(dir));
    }

    private ReloadResult reloadInternal(Path root) {
        ReloadResult result = new ReloadResult();

        if (root == null || !Files.exists(root) || !Files.isDirectory(root)) {
            result.addWarning("JDBC driver directory does not exist or is not a directory: " + root);
            return result;
        }

        List<Path> dbTypeDirs;
        try {
            dbTypeDirs = Files.list(root)
                    .filter(Files::isDirectory)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            result.addWarning("Failed to list JDBC driver directory: " + e.getMessage());
            return result;
        }

        for (Path dbDir : dbTypeDirs) {
            String dbType = dbDir.getFileName() != null ? dbDir.getFileName().toString() : "";
            if (dbType.isBlank()) {
                continue;
            }

            result.incrementDbTypesScanned();

            try {
                loadDbTypeDirectory(dbType, dbDir, result);
            } catch (Exception e) {
                result.addWarning("Failed to load dbType directory '" + dbType + "': " + e.getMessage());
            }
        }

        return result;
    }

    private void loadDbTypeDirectory(String dbType, Path dbDir, ReloadResult result) throws Exception {
        Path manifestPath = dbDir.resolve("driver.json");
        if (!Files.exists(manifestPath) || !Files.isRegularFile(manifestPath)) {
            if (BUILTIN_DB_TYPES.contains(dbType.toLowerCase(Locale.ROOT))) {
                log.debug("Skipping driver.json validation for builtin dbType '{}' under: {}", dbType, manifestPath);
                return;
            }

            result.addWarning("Missing driver.json for dbType '" + dbType + "' under: " + manifestPath);
            return;
        }

        DriverManifest manifest;
        try (InputStream in = Files.newInputStream(manifestPath)) {
            manifest = objectMapper.readValue(in, DriverManifest.class);
        }

        validateManifest(dbType, manifest);

        List<Path> jarFiles = listJarFiles(dbDir);
        if (jarFiles.isEmpty()) {
            if (BUILTIN_DB_TYPES.contains(dbType.toLowerCase(Locale.ROOT))) {
                log.debug("No JDBC driver JARs found for builtin dbType '{}' under: {}", dbType, dbDir);
                return;
            }

            result.addWarning("No JDBC driver JARs found for dbType '" + dbType + "' under: " + dbDir);
            return;
        }

        List<URL> urls = new ArrayList<>();
        for (Path jar : jarFiles) {
            urls.add(jar.toUri().toURL());
        }

        URLClassLoader loader = new URLClassLoader(urls.toArray(new URL[0]), JdbcDriverAutoLoader.class.getClassLoader());

        List<String> discovered = discoverDriverClasses(urls, loader);

        String driverClass = manifest.getDriverClass();
        if (driverClass == null || driverClass.isBlank()) {
            result.addWarning("Missing driverClass in driver.json for dbType '" + dbType + "'");
            return;
        }

        if (!driverRegistry.isDriverClassRegistered(driverClass)) {
            Class<?> clazz = Class.forName(driverClass, true, loader);
            Object obj = clazz.getDeclaredConstructor().newInstance();
            if (!(obj instanceof Driver)) {
                throw new IllegalArgumentException("Configured driverClass is not a java.sql.Driver: " + driverClass);
            }

            DriverManager.registerDriver(new DriverShim((Driver) obj));
            driverRegistry.markDriverClassRegistered(driverClass);
            result.incrementDriverClassesRegistered();
            log.info("Registered JDBC driver (dbType={}, driverClass={}, jars={})", dbType, driverClass, jarFiles.size());
        }

        List<String> jarPaths = jarFiles.stream().map(p -> p.toAbsolutePath().toString()).toList();
        DriverRegistry.Entry entry = new DriverRegistry.Entry(
                dbType.toLowerCase(Locale.ROOT),
                DriverRegistry.Source.DIRECTORY,
                manifest,
                jarPaths,
                discovered,
                OffsetDateTime.now(),
                loader
        );
        driverRegistry.upsertDirectoryEntry(entry);
    }

    private void validateManifest(String dbType, DriverManifest manifest) {
        if (manifest == null) {
            throw new IllegalArgumentException("driver.json is empty or invalid");
        }
        if (manifest.getDbType() == null || manifest.getDbType().isBlank()) {
            throw new IllegalArgumentException("driver.json missing dbType");
        }
        if (!manifest.getDbType().trim().equalsIgnoreCase(dbType)) {
            throw new IllegalArgumentException(
                    "driver.json dbType does not match directory name (dir=" + dbType + ", manifest=" + manifest.getDbType() + ")"
            );
        }
        if (manifest.getJdbcUrlTemplate() == null || manifest.getJdbcUrlTemplate().isBlank()) {
            throw new IllegalArgumentException("driver.json missing jdbcUrlTemplate");
        }
    }

    private List<Path> listJarFiles(Path dbDir) throws IOException {
        try (var stream = Files.walk(dbDir, 2)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName() != null)
                    .filter(p -> p.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".jar"))
                    .collect(Collectors.toList());
        }
    }

    private List<String> discoverDriverClasses(List<URL> jarUrls, ClassLoader loader) {
        List<String> discovered = new ArrayList<>();

        // Try META-INF/services/java.sql.Driver first.
        for (URL jarUrl : jarUrls) {
            try {
                URL serviceUrl = new URL("jar:" + jarUrl.toExternalForm() + "!/META-INF/services/java.sql.Driver");
                try (InputStream in = serviceUrl.openStream()) {
                    String content = new String(in.readAllBytes());
                    for (String line : content.split("\\r?\\n")) {
                        String v = line.trim();
                        if (!v.isBlank() && !v.startsWith("#")) {
                            discovered.add(v);
                        }
                    }
                }
            } catch (Exception ignored) {
                // best effort
            }
        }

        if (!discovered.isEmpty()) {
            return discovered.stream().distinct().toList();
        }

        // Fallback: ServiceLoader
        try {
            ServiceLoader<Driver> serviceLoader = ServiceLoader.load(Driver.class, loader);
            for (Driver d : serviceLoader) {
                if (d != null && d.getClass() != null) {
                    discovered.add(d.getClass().getName());
                }
            }
        } catch (Exception ignored) {
            // best effort
        }

        return discovered.stream().distinct().toList();
    }

    private String getTrimmed(String propKey, String envKey) {
        String v = null;
        if (environment != null && propKey != null) {
            v = environment.getProperty(propKey);
        }
        if ((v == null || v.isBlank()) && environment != null && envKey != null) {
            v = environment.getProperty(envKey);
        }
        return v != null ? v.trim() : null;
    }

    private boolean getBool(String propKey, String envKey, boolean defaultValue) {
        String v = getTrimmed(propKey, envKey);
        if (v == null || v.isBlank()) {
            return defaultValue;
        }
        String s = v.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(s) || "1".equals(s) || "yes".equals(s) || "y".equals(s)) {
            return true;
        }
        if ("false".equals(s) || "0".equals(s) || "no".equals(s) || "n".equals(s)) {
            return false;
        }
        return defaultValue;
    }

    /**
     * Result of a reload operation.
     */
    public static class ReloadResult {
        private int dbTypesScanned;
        private int driverClassesRegistered;
        private final List<String> warnings = new ArrayList<>();

        /**
         * Increment scanned dbType count.
         */
        public void incrementDbTypesScanned() {
            dbTypesScanned++;
        }

        /**
         * Increment registered driver class count.
         */
        public void incrementDriverClassesRegistered() {
            driverClassesRegistered++;
        }

        /**
         * Add warning.
         *
         * @param warning warning
         */
        public void addWarning(String warning) {
            if (warning != null && !warning.isBlank()) {
                warnings.add(warning);
            }
        }

        /**
         * Get dbType scanned count.
         *
         * @return count
         */
        public int getDbTypesScanned() {
            return dbTypesScanned;
        }

        /**
         * Get number of newly registered driver classes.
         *
         * @return count
         */
        public int getDriverClassesRegistered() {
            return driverClassesRegistered;
        }

        /**
         * Get warnings.
         *
         * @return warnings
         */
        public List<String> getWarnings() {
            return List.copyOf(warnings);
        }

        /**
         * Convert to a response map friendly structure.
         *
         * @return map
         */
        public Map<String, Object> toMap() {
            Map<String, Object> m = new HashMap<>();
            m.put("db_types_scanned", dbTypesScanned);
            m.put("driver_classes_registered", driverClassesRegistered);
            m.put("warnings", List.copyOf(warnings));
            return m;
        }
    }
}
