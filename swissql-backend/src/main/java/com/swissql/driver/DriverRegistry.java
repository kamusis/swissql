package com.swissql.driver;

import org.springframework.stereotype.Component;

import java.net.URLClassLoader;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory registry of JDBC drivers known to the backend.
 *
 * Entries are populated from:
 * - Built-in drivers (Oracle/Postgres)
 * - Directory-provided drivers under {@code /jdbc_drivers/<dbType>/}
 */
@Component
public class DriverRegistry {

    /**
     * Describes where a driver came from.
     */
    public enum Source {
        BUILTIN,
        DIRECTORY
    }

    /**
     * A registry entry for one dbType.
     */
    public static class Entry {
        private final String dbType;
        private final Source source;
        private final DriverManifest manifest;
        private final List<String> jarPaths;
        private final List<String> discoveredDriverClasses;
        private final OffsetDateTime lastLoadedAt;
        private final URLClassLoader classLoader;

        /**
         * Create a registry entry.
         *
         * @param dbType dbType
         * @param source source
         * @param manifest manifest (may be null for builtin)
         * @param jarPaths jar paths
         * @param discoveredDriverClasses discovered driver classes
         * @param lastLoadedAt load timestamp
         * @param classLoader classloader used to load jars (may be null for builtin)
         */
        public Entry(
                String dbType,
                Source source,
                DriverManifest manifest,
                List<String> jarPaths,
                List<String> discoveredDriverClasses,
                OffsetDateTime lastLoadedAt,
                URLClassLoader classLoader
        ) {
            this.dbType = dbType;
            this.source = source;
            this.manifest = manifest;
            this.jarPaths = jarPaths != null ? List.copyOf(jarPaths) : List.of();
            this.discoveredDriverClasses = discoveredDriverClasses != null ? List.copyOf(discoveredDriverClasses) : List.of();
            this.lastLoadedAt = lastLoadedAt;
            this.classLoader = classLoader;
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
         * Get driver source.
         *
         * @return source
         */
        public Source getSource() {
            return source;
        }

        /**
         * Get driver manifest.
         *
         * @return manifest
         */
        public DriverManifest getManifest() {
            return manifest;
        }

        /**
         * Get jar paths.
         *
         * @return jar paths
         */
        public List<String> getJarPaths() {
            return jarPaths;
        }

        /**
         * Get discovered driver classes.
         *
         * @return driver classes
         */
        public List<String> getDiscoveredDriverClasses() {
            return discoveredDriverClasses;
        }

        /**
         * Get last load time.
         *
         * @return last load time
         */
        public OffsetDateTime getLastLoadedAt() {
            return lastLoadedAt;
        }

        /**
         * Get classloader.
         *
         * @return classloader
         */
        public URLClassLoader getClassLoader() {
            return classLoader;
        }
    }

    private final Map<String, Entry> entriesByDbType = new ConcurrentHashMap<>();
    private final Set<String> registeredDriverClasses = ConcurrentHashMap.newKeySet();

    /**
     * Register a built-in dbType.
     *
     * @param dbType dbType
     * @param driverClass driver class
     */
    public void registerBuiltin(String dbType, String driverClass) {
        DriverManifest manifest = new DriverManifest();
        manifest.setDbType(dbType);
        manifest.setDriverClass(driverClass);
        entriesByDbType.put(dbType.toLowerCase(), new Entry(
                dbType.toLowerCase(),
                Source.BUILTIN,
                manifest,
                List.of(),
                List.of(driverClass),
                OffsetDateTime.now(),
                null
        ));
    }

    /**
     * Upsert a directory entry.
     *
     * @param entry entry
     */
    public void upsertDirectoryEntry(Entry entry) {
        if (entry == null || entry.getDbType() == null) {
            return;
        }
        entriesByDbType.put(entry.getDbType().toLowerCase(), entry);
    }

    /**
     * Register an alias dbType entry that resolves to the provided canonical entry.
     *
     * @param alias alias dbType
     * @param canonicalEntry canonical entry
     */
    public void upsertAlias(String alias, Entry canonicalEntry) {
        if (alias == null || alias.isBlank() || canonicalEntry == null) {
            return;
        }
        String normalizedAlias = alias.trim().toLowerCase();
        if (normalizedAlias.isBlank()) {
            return;
        }
        if (normalizedAlias.equalsIgnoreCase(canonicalEntry.getDbType())) {
            return;
        }

        Entry aliasEntry = new Entry(
                normalizedAlias,
                canonicalEntry.getSource(),
                canonicalEntry.getManifest(),
                canonicalEntry.getJarPaths(),
                canonicalEntry.getDiscoveredDriverClasses(),
                canonicalEntry.getLastLoadedAt(),
                canonicalEntry.getClassLoader()
        );
        entriesByDbType.put(normalizedAlias, aliasEntry);
    }

    /**
     * List all driver entries.
     *
     * @return entries
     */
    public List<Entry> list() {
        List<Entry> entries = new ArrayList<>(entriesByDbType.values());
        entries.sort((a, b) -> a.getDbType().compareToIgnoreCase(b.getDbType()));
        return Collections.unmodifiableList(entries);
    }

    /**
     * Find an entry by dbType.
     *
     * @param dbType dbType
     * @return entry
     */
    public Optional<Entry> find(String dbType) {
        if (dbType == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(entriesByDbType.get(dbType.toLowerCase()));
    }

    /**
     * Check whether a driver class has already been registered in this process.
     *
     * @param driverClass driver class
     * @return true if already registered
     */
    public boolean isDriverClassRegistered(String driverClass) {
        return driverClass != null && registeredDriverClasses.contains(driverClass);
    }

    /**
     * Mark a driver class as registered.
     *
     * @param driverClass driver class
     */
    public void markDriverClassRegistered(String driverClass) {
        if (driverClass != null) {
            registeredDriverClasses.add(driverClass);
        }
    }
}
