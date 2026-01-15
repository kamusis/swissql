package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
)

type completionCache struct {
	Tables    map[string][]string // key: schema
	Views     map[string][]string // key: schema
	Schemas   []string
	Columns   map[string][]string // key: schema.table
	ExpiresAt time.Time
}

var (
	cache      *completionCache
	cacheMutex sync.RWMutex
	cacheTTL   = 5 * time.Minute
)

func initCache() {
	cache = &completionCache{
		Tables:  make(map[string][]string),
		Views:   make(map[string][]string),
		Columns: make(map[string][]string),
	}
}

func isCacheValid() bool {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	return cache != nil && time.Now().Before(cache.ExpiresAt)
}

func refreshCache(c *client.Client, sessionId string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	if cache == nil {
		initCache()
	}

	// Refresh tables
	tablesResp, _ := c.MetaCompletions(sessionId, "tables", "", "", "")
	if tablesResp != nil && tablesResp.Data.Rows != nil {
		cache.Tables = make(map[string][]string)
		for _, row := range tablesResp.Data.Rows {
			if name, ok := row["name"].(string); ok {
				cache.Tables[""] = append(cache.Tables[""], name)
			}
		}
	}

	// Refresh views
	viewsResp, _ := c.MetaCompletions(sessionId, "views", "", "", "")
	if viewsResp != nil && viewsResp.Data.Rows != nil {
		cache.Views = make(map[string][]string)
		for _, row := range viewsResp.Data.Rows {
			if name, ok := row["name"].(string); ok {
				cache.Views[""] = append(cache.Views[""], name)
			}
		}
	}

	// Refresh schemas
	schemasResp, _ := c.MetaCompletions(sessionId, "schemas", "", "", "")
	if schemasResp != nil && schemasResp.Data.Rows != nil {
		cache.Schemas = []string{}
		for _, row := range schemasResp.Data.Rows {
			if name, ok := row["name"].(string); ok {
				cache.Schemas = append(cache.Schemas, name)
			}
		}
	}

	cache.ExpiresAt = time.Now().Add(cacheTTL)
}

func getCachedTables() []string {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	if cache == nil {
		return nil
	}
	return cache.Tables[""]
}

func getCachedViews() []string {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	if cache == nil {
		return nil
	}
	return cache.Views[""]
}

func getCachedSchemas() []string {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	if cache == nil {
		return nil
	}
	return cache.Schemas
}

func getCachedColumns(tableName string) []string {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	if cache == nil {
		return nil
	}
	return cache.Columns[tableName]
}

func setCachedColumns(tableName string, columns []string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	if cache == nil {
		initCache()
	}
	cache.Columns[tableName] = columns
}

func invalidateCache() {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	cache = nil
}

func filterCompletions(candidates []string, prefix string) []string {
	if prefix == "" {
		return candidates
	}

	var matches []string
	lowerPrefix := strings.ToLower(prefix)
	for _, c := range candidates {
		if strings.HasPrefix(strings.ToLower(c), lowerPrefix) {
			matches = append(matches, c)
		}
	}
	return matches
}

func makeLineCompletions(line string, lastWord string, candidates []string, appendSpace bool) []string {
	if len(candidates) == 0 {
		return nil
	}

	trimmedRight := strings.TrimRight(line, " \t")
	if lastWord == "" {
		prefix := line
		out := make([]string, 0, len(candidates))
		for _, c := range candidates {
			if appendSpace {
				out = append(out, prefix+c+" ")
			} else {
				out = append(out, prefix+c)
			}
		}
		return out
	}

	// Find the start offset of lastWord based on the right-trimmed line.
	// This avoids incorrect offsets when the original line contains trailing spaces.
	idx := strings.LastIndex(trimmedRight, lastWord)
	if idx < 0 {
		start := len(trimmedRight) - len(lastWord)
		if start < 0 || start > len(trimmedRight) {
			return candidates
		}
		idx = start
	}
	prefix := trimmedRight[:idx]

	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if appendSpace {
			out = append(out, prefix+c+" ")
		} else {
			out = append(out, prefix+c)
		}
	}
	return out
}

func makeCompleter(c *client.Client, sessionId string) func(string) []string {
	return func(line string) []string {
		if line == "" {
			base := []string{
				"help", "detach", "exit", "quit",
				"set display wide|narrow", "set display expanded on|off", "set display width",
				"set dbtype",
				"set output table|csv|tsv|json",
				"list drivers", "reload drivers",
				"connect",
				"/ai",
			}
			if strings.TrimSpace(sessionId) == "" {
				return base
			}
			return append(base,
				"\\d", "\\d+", "\\dt", "\\dv", "\\i", "\\explain", "\\conninfo", "\\x", "\\timing", "\\watch",
				"desc", "desc+", "explain", "explain plan for", "explain analyze",
				"@<file>",
				"/context",
				"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
				"FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY",
			)
		}

		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			return nil
		}

		// Get the last word being typed.
		// If the user has just typed a space, treat it as starting a new token
		// (lastWord="") so we don't attempt to replace the previous token and
		// accidentally create artifacts like "FFROM".
		words := strings.Fields(trimmedLine)
		if len(words) == 0 {
			return nil
		}

		hasTrailingSpace := strings.HasSuffix(line, " ") || strings.HasSuffix(line, "\t")
		lastWord := words[len(words)-1]
		prevWord := ""
		if hasTrailingSpace {
			prevWord = strings.ToUpper(lastWord)
			lastWord = ""
		} else if len(words) > 1 {
			prevWord = strings.ToUpper(words[len(words)-2])
		}

		// Check for specific command contexts
		switch {
		case strings.HasPrefix(lastWord, "\\"):
			// Meta command completion
			metaCommands := []string{
				"\\d", "\\d+", "\\dt", "\\dv", "\\i", "\\explain", "\\explain analyze", "\\conninfo", "\\x", "\\timing", "\\watch",
				"\\sampler",
			}
			return makeLineCompletions(line, lastWord, filterCompletions(metaCommands, lastWord), true)

		case strings.HasPrefix(lastWord, "/"):
			// AI command completion
			aiCommands := []string{"/ai", "/context"}
			return makeLineCompletions(line, lastWord, filterCompletions(aiCommands, lastWord), true)

		case strings.EqualFold(prevWord, "\\i") || strings.HasPrefix(prevWord, "@"):
			// File path completion
			return makeLineCompletions(line, lastWord, completeFilePath(lastWord), false)

		case strings.TrimSpace(sessionId) != "" && (strings.EqualFold(prevWord, "\\d") || strings.EqualFold(prevWord, "\\d+") || strings.EqualFold(prevWord, "desc") || strings.EqualFold(prevWord, "desc+")):
			// Table/view name completion
			return makeLineCompletions(line, lastWord, completeTableNames(c, sessionId, lastWord), true)

		case strings.TrimSpace(sessionId) != "" && strings.EqualFold(prevWord, "\\dt"):
			// Schema name completion for \dt
			return makeLineCompletions(line, lastWord, completeSchemaNames(c, sessionId, lastWord), true)

		case strings.TrimSpace(sessionId) != "" && strings.EqualFold(prevWord, "\\dv"):
			// Schema name completion for \dv
			return makeLineCompletions(line, lastWord, completeSchemaNames(c, sessionId, lastWord), true)

		case strings.EqualFold(prevWord, "\\explain") || strings.EqualFold(prevWord, "explain"):
			// SQL keyword completion (including analyze)
			sqlKeywords := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "analyze", "plan for"}
			return makeLineCompletions(line, lastWord, filterCompletions(sqlKeywords, lastWord), true)

		case strings.EqualFold(prevWord, "analyze") && (strings.EqualFold(words[0], "\\explain") || strings.EqualFold(words[0], "explain")):
			// After "explain analyze", complete SQL keywords
			sqlKeywords := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP"}
			return makeLineCompletions(line, lastWord, filterCompletions(sqlKeywords, lastWord), true)

		case strings.EqualFold(prevWord, "\\x") || strings.EqualFold(prevWord, "\\timing"):
			// on/off completion
			options := []string{"on", "off"}
			return makeLineCompletions(line, lastWord, filterCompletions(options, lastWord), true)

		case strings.EqualFold(prevWord, "/context"):
			// context subcommand completion
			options := []string{"show", "clear"}
			return makeLineCompletions(line, lastWord, filterCompletions(options, lastWord), true)

		case strings.EqualFold(prevWord, "DBTYPE") && len(words) >= 2 && strings.EqualFold(words[0], "set"):
			drivers, err := c.MetaDrivers()
			if err != nil || drivers == nil {
				return nil
			}
			options := make([]string, 0, len(drivers.Drivers))
			seen := map[string]bool{}
			for _, d := range drivers.Drivers {
				k := strings.ToLower(strings.TrimSpace(d.DbType))
				if k == "" || seen[k] {
					continue
				}
				seen[k] = true
				options = append(options, k)
			}
			return makeLineCompletions(line, lastWord, filterCompletions(options, lastWord), true)

		case strings.TrimSpace(sessionId) != "" && (prevWord == "SELECT" || prevWord == "INSERT" || prevWord == "UPDATE" || prevWord == "DELETE"):
			// SQL keyword completion
			sqlKeywords := []string{"FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY"}
			keywordMatches := filterCompletions(sqlKeywords, lastWord)
			// Also try column name completion
			columnMatches := completeColumnNames(c, sessionId, lastWord, trimmedLine)
			// Combine both
			return makeLineCompletions(line, lastWord, append(keywordMatches, columnMatches...), true)

		case strings.TrimSpace(sessionId) != "" && (prevWord == "FROM" || prevWord == "JOIN"):
			// Table/view name completion
			return makeLineCompletions(line, lastWord, completeTableNames(c, sessionId, lastWord), true)

		case strings.TrimSpace(sessionId) != "" && (prevWord == "WHERE" || prevWord == "GROUP BY" || prevWord == "ORDER BY"):
			// Column name completion
			return makeLineCompletions(line, lastWord, completeColumnNames(c, sessionId, lastWord, trimmedLine), true)

		default:
			// General SQL keyword completion
			sqlKeywords := []string{
				"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
				"FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY", "AND", "OR", "NOT",
				"\\d", "\\d+", "\\dt", "\\dv", "\\i", "\\explain", "\\explain analyze", "\\conninfo", "\\x", "\\timing",
				"\\sampler",
				"desc", "desc+", "explain", "explain plan for", "explain analyze", "help", "detach", "exit", "quit",
				"/ai", "/context",
			}
			return makeLineCompletions(line, lastWord, filterCompletions(sqlKeywords, lastWord), true)
		}
	}
}

func completeTableNames(c *client.Client, sessionId string, prefix string) []string {
	schemaPrefix := ""
	namePrefix := prefix
	if dot := strings.LastIndex(prefix, "."); dot > 0 && dot < len(prefix)-1 {
		schemaPrefix = prefix[:dot]
		namePrefix = prefix[dot+1:]
	}

	// Try cache first
	if schemaPrefix == "" && isCacheValid() {
		tables := getCachedTables()
		views := getCachedViews()
		allNames := append(tables, views...)
		return filterCompletions(allNames, namePrefix)
	}

	// Query both tables and views
	tablesResp, err := c.MetaCompletions(sessionId, "tables", schemaPrefix, "", namePrefix)
	if err != nil {
		return nil
	}

	viewsResp, err := c.MetaCompletions(sessionId, "views", schemaPrefix, "", namePrefix)
	if err != nil {
		return nil
	}

	var names []string
	if tablesResp != nil && tablesResp.Data.Rows != nil {
		for _, row := range tablesResp.Data.Rows {
			if name, ok := row["name"].(string); ok {
				names = append(names, name)
			}
		}
	}

	if viewsResp != nil && viewsResp.Data.Rows != nil {
		for _, row := range viewsResp.Data.Rows {
			if name, ok := row["name"].(string); ok {
				names = append(names, name)
			}
		}
	}

	// Refresh cache in background if prefix is empty
	if schemaPrefix == "" && namePrefix == "" {
		go refreshCache(c, sessionId)
	}

	if schemaPrefix == "" {
		return names
	}

	qualified := make([]string, 0, len(names))
	for _, n := range names {
		qualified = append(qualified, schemaPrefix+"."+n)
	}
	return qualified
}

func completeSchemaNames(c *client.Client, sessionId string, prefix string) []string {
	// Try cache first
	if isCacheValid() {
		schemas := getCachedSchemas()
		return filterCompletions(schemas, prefix)
	}

	resp, err := c.MetaCompletions(sessionId, "schemas", "", "", prefix)
	if err != nil {
		return nil
	}

	var names []string
	if resp != nil && resp.Data.Rows != nil {
		for _, row := range resp.Data.Rows {
			if name, ok := row["name"].(string); ok {
				names = append(names, name)
			}
		}
	}

	// Refresh cache in background if prefix is empty
	if prefix == "" {
		go refreshCache(c, sessionId)
	}

	return names
}

func completeFilePath(prefix string) []string {
	dir, filePrefix := filepath.Split(prefix)
	if dir == "" {
		dir = "."
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	var matches []string
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, filePrefix) {
			fullPath := filepath.Join(dir, name)
			if entry.IsDir() {
				fullPath += string(filepath.Separator)
			}
			matches = append(matches, fullPath)
		}
	}
	return matches
}

func extractTableNameFromSQL(sql string) string {
	// Simple extraction: look for "FROM <table>" or "JOIN <table>"
	upperSQL := strings.ToUpper(sql)

	// Try to find FROM clause
	fromIdx := strings.Index(upperSQL, " FROM ")
	if fromIdx != -1 {
		fromClause := upperSQL[fromIdx+6:]
		// Extract the first word after FROM
		words := strings.Fields(fromClause)
		if len(words) > 0 {
			return words[0]
		}
	}

	// Try to find JOIN clause
	joinIdx := strings.Index(upperSQL, " JOIN ")
	if joinIdx != -1 {
		joinClause := upperSQL[joinIdx+6:]
		words := strings.Fields(joinClause)
		if len(words) > 0 {
			return words[0]
		}
	}

	return ""
}

func completeColumnNames(c *client.Client, sessionId string, prefix string, sql string) []string {
	tableName := extractTableNameFromSQL(sql)
	if tableName == "" {
		return nil
	}

	// Try cache first
	if isCacheValid() {
		columns := getCachedColumns(tableName)
		if columns != nil {
			return filterCompletions(columns, prefix)
		}
	}

	resp, err := c.MetaCompletions(sessionId, "columns", "", tableName, prefix)
	if err != nil {
		return nil
	}

	var names []string
	if resp != nil && resp.Data.Rows != nil {
		for _, row := range resp.Data.Rows {
			if name, ok := row["name"].(string); ok {
				names = append(names, name)
			}
		}
	}

	// Cache the result if prefix is empty (full list)
	if prefix == "" && len(names) > 0 {
		setCachedColumns(tableName, names)
	}

	return names
}
