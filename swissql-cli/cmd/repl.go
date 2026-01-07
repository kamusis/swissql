package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kamusis/swissql/swissql-cli/internal/client"
	"github.com/kamusis/swissql/swissql-cli/internal/config"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

type completionCache struct {
	Tables    map[string][]string // key: schema
	Views     map[string][]string // key: schema
	Schemas   []string
	Columns   map[string][]string // key: schema.table
	ExpiresAt time.Time
	mu        sync.RWMutex
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

func isMetaCommandStart(s string) bool {
	if s == "" {
		return false
	}
	return strings.HasPrefix(s, "\\") || strings.HasPrefix(strings.ToLower(s), "desc") || strings.HasPrefix(strings.ToLower(s), "explain") || strings.HasPrefix(s, "@") || strings.EqualFold(s, "conninfo")
}

func parseMetaCommand(input string) (string, []string) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return "", nil
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return "", nil
	}
	cmd := fields[0]
	args := []string{}
	if len(fields) > 1 {
		args = fields[1:]
	}
	return cmd, args
}

func readFileContent(baseDir string, path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("file path is required")
	}
	p := path
	if baseDir != "" && !filepath.IsAbs(path) {
		p = filepath.Join(baseDir, path)
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func trimTrailingSemicolon(s string) string {
	t := strings.TrimSpace(s)
	if strings.HasSuffix(t, ";") {
		return strings.TrimSpace(strings.TrimSuffix(t, ";"))
	}
	return t
}

func shouldRecordHistory(mode string, input string, isSql bool) bool {
	if isSql {
		return true
	}

	s := strings.TrimSpace(input)
	if s == "" {
		return false
	}

	sLower := strings.ToLower(s)

	switch mode {
	case "all":
		return true
	case "sql_only":
		return false
	case "safe_only":
		if strings.HasPrefix(sLower, "/ai") || strings.HasPrefix(sLower, "/context") {
			return false
		}
		if strings.HasPrefix(s, "@") || strings.HasPrefix(s, "\\i") {
			return false
		}
		if strings.HasPrefix(s, "\\d") || strings.HasPrefix(sLower, "desc") {
			return true
		}
		if strings.HasPrefix(s, "\\dt") || strings.HasPrefix(s, "\\dv") {
			return true
		}
		if strings.HasPrefix(s, "\\explain") || strings.HasPrefix(sLower, "explain") {
			return true
		}
		if sLower == "conninfo" || sLower == "\\conninfo" {
			return true
		}
		if strings.HasPrefix(sLower, "set display ") {
			return true
		}
		if strings.HasPrefix(sLower, "set output ") {
			return true
		}
		if strings.HasPrefix(s, "\\x") {
			return true
		}
		if strings.HasPrefix(s, "\\o") {
			return true
		}
		return false
	default:
		return false
	}
}

func myCompleter(line string) []string {
	if line == "" {
		return []string{
			"\\d", "\\d+", "\\dt", "\\dv", "\\i", "\\explain", "\\conninfo", "\\x", "\\timing", "\\watch",
			"desc", "desc+", "explain", "help", "detach", "exit", "quit",
			"/ai", "/context",
			"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
			"FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY",
		}
	}

	trimmedLine := strings.TrimSpace(line)
	if trimmedLine == "" {
		return nil
	}

	// Get the last word being typed
	words := strings.Fields(trimmedLine)
	if len(words) == 0 {
		return nil
	}

	lastWord := words[len(words)-1]
	prevWord := ""
	if len(words) > 1 {
		prevWord = strings.ToUpper(words[len(words)-2])
	}

	// Check for specific command contexts
	switch {
	case strings.HasPrefix(lastWord, "\\"):
		// Meta command completion
		metaCommands := []string{
			"\\d", "\\d+", "\\dt", "\\dv", "\\i", "\\explain", "\\conninfo", "\\x", "\\timing", "\\watch",
		}
		return filterCompletions(metaCommands, lastWord)

	case strings.HasPrefix(lastWord, "/"):
		// AI command completion
		aiCommands := []string{"/ai", "/context"}
		return filterCompletions(aiCommands, lastWord)

	case strings.EqualFold(prevWord, "\\i") || strings.HasPrefix(prevWord, "@"):
		// File path completion - handled separately in Phase 2
		return nil

	case strings.EqualFold(prevWord, "\\d") || strings.EqualFold(prevWord, "desc"):
		// Table/view name completion - handled separately in Phase 2
		return nil

	case strings.EqualFold(prevWord, "\\explain") || strings.EqualFold(prevWord, "explain"):
		// SQL keyword completion
		sqlKeywords := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP"}
		return filterCompletions(sqlKeywords, lastWord)

	case strings.EqualFold(prevWord, "\\x") || strings.EqualFold(prevWord, "\\timing"):
		// on/off completion
		options := []string{"on", "off"}
		return filterCompletions(options, lastWord)

	case strings.EqualFold(prevWord, "/context"):
		// context subcommand completion
		options := []string{"show", "clear"}
		return filterCompletions(options, lastWord)

	case prevWord == "SELECT" || prevWord == "INSERT" || prevWord == "UPDATE" || prevWord == "DELETE":
		// SQL keyword completion
		sqlKeywords := []string{"FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY"}
		return filterCompletions(sqlKeywords, lastWord)

	case prevWord == "FROM" || prevWord == "JOIN":
		// Table/view name completion - handled separately in Phase 3
		return nil

	default:
		// General SQL keyword completion
		sqlKeywords := []string{
			"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
			"FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY", "AND", "OR", "NOT",
		}
		return filterCompletions(sqlKeywords, lastWord)
	}
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
			return []string{
				"\\d", "\\d+", "\\dt", "\\dv", "\\i", "\\explain", "\\conninfo", "\\x", "\\timing", "\\watch",
				"desc", "desc+", "explain", "explain plan for", "explain analyze", "help", "detach", "exit", "quit",
				"@<file>", "set display expanded on|off",
				"/ai", "/context",
				"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
				"FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY",
			}
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
			}
			return makeLineCompletions(line, lastWord, filterCompletions(metaCommands, lastWord), true)

		case strings.HasPrefix(lastWord, "/"):
			// AI command completion
			aiCommands := []string{"/ai", "/context"}
			return makeLineCompletions(line, lastWord, filterCompletions(aiCommands, lastWord), true)

		case strings.EqualFold(prevWord, "\\i") || strings.HasPrefix(prevWord, "@"):
			// File path completion
			return makeLineCompletions(line, lastWord, completeFilePath(lastWord), false)

		case strings.EqualFold(prevWord, "\\d") || strings.EqualFold(prevWord, "\\d+") || strings.EqualFold(prevWord, "desc") || strings.EqualFold(prevWord, "desc+"):
			// Table/view name completion
			return makeLineCompletions(line, lastWord, completeTableNames(c, sessionId, lastWord), true)

		case strings.EqualFold(prevWord, "\\dt"):
			// Schema name completion for \dt
			return makeLineCompletions(line, lastWord, completeSchemaNames(c, sessionId, lastWord), true)

		case strings.EqualFold(prevWord, "\\dv"):
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

		case prevWord == "SELECT" || prevWord == "INSERT" || prevWord == "UPDATE" || prevWord == "DELETE":
			// SQL keyword completion
			sqlKeywords := []string{"FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY"}
			keywordMatches := filterCompletions(sqlKeywords, lastWord)
			// Also try column name completion
			columnMatches := completeColumnNames(c, sessionId, lastWord, trimmedLine)
			// Combine both
			return makeLineCompletions(line, lastWord, append(keywordMatches, columnMatches...), true)

		case prevWord == "FROM" || prevWord == "JOIN":
			// Table/view name completion
			return makeLineCompletions(line, lastWord, completeTableNames(c, sessionId, lastWord), true)

		case prevWord == "WHERE" || prevWord == "GROUP BY" || prevWord == "ORDER BY":
			// Column name completion
			return makeLineCompletions(line, lastWord, completeColumnNames(c, sessionId, lastWord, trimmedLine), true)

		default:
			// General SQL keyword completion
			sqlKeywords := []string{
				"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER",
				"FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY", "AND", "OR", "NOT",
				"\\d", "\\d+", "\\dt", "\\dv", "\\i", "\\explain", "\\explain analyze", "\\conninfo", "\\x", "\\timing",
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

var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "Start an interactive SQL REPL",
	RunE: func(cmd *cobra.Command, args []string) error {
		name, _ := cmd.Flags().GetString("name")
		entry, err := config.ResolveActiveSession(name)
		if err != nil {
			return err
		}

		if name != "" {
			cfg, err := config.LoadConfig()
			if err != nil {
				return err
			}
			cfg.CurrentName = name
			_ = config.SaveConfig(cfg)
			_ = config.TouchSession(name)
		}

		server := entry.ServerURL
		timeout, _ := cmd.Flags().GetInt("connection-timeout")
		c := client.NewClient(server, time.Duration(timeout)*time.Millisecond)

		// Load persisted CLI display settings
		cfg, err := config.LoadConfig()
		if err == nil && cfg != nil {
			setDisplayWide(cfg.DisplayWide)
			setDisplayExpanded(cfg.DisplayExpanded)
			setDisplayWidth(cfg.Display.MaxColWidth)
			setDisplayQueryWidth(cfg.Display.MaxQueryWidth)
			_ = setOutputFormat(cfg.OutputFormat)
		}

		line := liner.NewLiner()
		defer line.Close()

		line.SetCtrlCAborts(true)
		line.SetCompleter(makeCompleter(c, entry.SessionId))

		historyPath, _ := config.GetHistoryPath()
		if f, err := os.Open(historyPath); err == nil {
			line.ReadHistory(f)
			f.Close()
		}

		fmt.Printf("SwissQL REPL (Session: %s)\n", entry.SessionId)
		fmt.Println("Type 'help' for commands. Use 'detach' to leave without disconnecting.")
		fmt.Println("Type 'exit' or 'quit' to disconnect and remove this session.")
		fmt.Println("Use '/ai <prompt>' to generate SQL via backend and confirm before execution.")

		var multiLineSql []string

		for {
			prompt := "swissql> "
			if len(multiLineSql) > 0 {
				prompt = "      -> "
			}

			input, err := line.Prompt(prompt)
			if err != nil {
				if err == liner.ErrPromptAborted {
					fmt.Println("^C")
					multiLineSql = nil
					continue
				}
				return err
			}

			input = strings.TrimSpace(input)
			if input == "" {
				continue
			}

			lower := strings.ToLower(input)
			if lower == "help" {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				fmt.Println("Commands:")
				fmt.Println("")
				fmt.Println("[CLI]")
				fmt.Println("  help                          Show this help")
				fmt.Println("  detach                        Leave REPL without disconnecting (like tmux detach)")
				fmt.Println("  exit | quit                   Disconnect backend session and remove it from registry")
				fmt.Println("  set display wide|narrow       Toggle truncation mode for tabular output")
				fmt.Println("  set display expanded on|off   Expanded display mode")
				fmt.Println("  set display width <n>         Set max column width for tabular output")
				fmt.Println("  set output table|csv|tsv|json Set output format")
				fmt.Println("")
				fmt.Println("[psql-compat (\\)]")
				fmt.Println("  \\conninfo                    Show current session and backend information")
				fmt.Println("  \\d <name> (alias: desc)       Describe a table/view")
				fmt.Println("  \\d+ <name> (alias: desc+)     Describe with more details")
				fmt.Println("  \\dt | \\dv                     List tables/views")
				fmt.Println("  \\explain <sql> (alias: explain, explain plan for)")
				fmt.Println("                               Show execution plan")
				fmt.Println("  \\explain analyze <sql> (alias: explain analyze)")
				fmt.Println("                               Show actual execution plan (executes the statement)")
				fmt.Println("  \\i <file> (alias: @<file>)    Execute statements from a file")
				fmt.Println("  \\x [on|off]                   Expanded display mode (like psql \\\\x)")
				fmt.Println("  \\o <file>                     Redirect query output to a file")
				fmt.Println("  \\o                            Restore output to stdout")
				fmt.Println("")
				fmt.Println("[AI (/)]")
				fmt.Println("  /ai <prompt>                  Generate SQL via AI and confirm before execution")
				fmt.Println("  /context show                 Show recent executed SQL context used by AI")
				fmt.Println("  /context clear                Clear AI context")
				fmt.Println("")
				fmt.Println("Notes:")
				fmt.Println("  - End a statement with ';' to execute")
				continue
			}
			if lower == "detach" {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				break
			}
			if lower == "exit" || lower == "quit" {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				if err := c.Disconnect(entry.SessionId); err != nil {
					fmt.Printf("%v\n", err)
				}

				resolvedName := name
				if strings.TrimSpace(resolvedName) == "" && cfg != nil {
					resolvedName = cfg.CurrentName
				}
				resolvedName = strings.TrimSpace(resolvedName)

				if resolvedName != "" {
					reg, err := config.LoadRegistry()
					if err == nil && reg != nil {
						reg.RemoveSession(resolvedName)
						_ = config.SaveRegistry(reg)
					}
					if cfg != nil && cfg.CurrentName == resolvedName {
						cfg.CurrentName = ""
						_ = config.SaveConfig(cfg)
					}
				}
				break
			}
			if strings.HasPrefix(lower, "set display ") {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				args := strings.Fields(lower)
				if len(args) == 3 {
					switch args[2] {
					case "wide":
						setDisplayWide(true)
						if cfg != nil {
							cfg.DisplayWide = true
							_ = config.SaveConfig(cfg)
						}
						fmt.Println("Display mode set to wide.")
						continue
					case "narrow":
						setDisplayWide(false)
						if cfg != nil {
							cfg.DisplayWide = false
							_ = config.SaveConfig(cfg)
						}
						fmt.Println("Display mode set to narrow.")
						continue
					}
				}
				if len(args) == 4 && args[2] == "expanded" {
					switch args[3] {
					case "on":
						setDisplayExpanded(true)
						if cfg != nil {
							cfg.DisplayExpanded = true
							_ = config.SaveConfig(cfg)
						}
						fmt.Println("Expanded display mode enabled.")
						continue
					case "off":
						setDisplayExpanded(false)
						if cfg != nil {
							cfg.DisplayExpanded = false
							_ = config.SaveConfig(cfg)
						}
						fmt.Println("Expanded display mode disabled.")
						continue
					}
				}
				if len(args) == 4 && args[2] == "width" {
					w, err := parseDisplayWidthArg(args[3])
					if err != nil {
						fmt.Println("Error: invalid width")
						continue
					}
					setDisplayWidth(w)
					if cfg != nil {
						cfg.Display.MaxColWidth = displayMaxColWidth
						_ = config.SaveConfig(cfg)
					}
					fmt.Printf("Display column width set to %d.\n", displayMaxColWidth)
					continue
				}
				fmt.Println("Usage: set display wide|narrow|expanded on|off|width <n>")
				continue
			}

			if strings.HasPrefix(lower, "set output ") {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				args := strings.Fields(lower)
				if len(args) == 3 {
					if err := setOutputFormat(args[2]); err != nil {
						fmt.Printf("Error: %v\n", err)
						continue
					}
					if cfg != nil {
						cfg.OutputFormat = strings.ToLower(strings.TrimSpace(args[2]))
						_ = config.SaveConfig(cfg)
					}
					fmt.Printf("Output format set to %s.\n", strings.ToLower(strings.TrimSpace(args[2])))
					continue
				}
				fmt.Println("Usage: set output table|csv|tsv|json")
				continue
			}

			// Phase 3 P0 meta-commands (single-line)
			if isMetaCommandStart(input) {
				cmdName, args := parseMetaCommand(input)
				cmdLower := strings.ToLower(cmdName)

				switch {
				case cmdLower == "\\x":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					if len(args) == 0 {
						setDisplayExpanded(!displayExpanded)
						if cfg != nil {
							cfg.DisplayExpanded = displayExpanded
							_ = config.SaveConfig(cfg)
						}
						if displayExpanded {
							fmt.Println("Expanded display mode enabled.")
						} else {
							fmt.Println("Expanded display mode disabled.")
						}
						continue
					}
					switch strings.ToLower(strings.TrimSpace(args[0])) {
					case "on":
						setDisplayExpanded(true)
						if cfg != nil {
							cfg.DisplayExpanded = true
							_ = config.SaveConfig(cfg)
						}
						fmt.Println("Expanded display mode enabled.")
						continue
					case "off":
						setDisplayExpanded(false)
						if cfg != nil {
							cfg.DisplayExpanded = false
							_ = config.SaveConfig(cfg)
						}
						fmt.Println("Expanded display mode disabled.")
						continue
					default:
						fmt.Println("Usage: \\x [on|off]")
						continue
					}

				case cmdLower == "\\o":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					if len(args) == 0 {
						if err := resetOutputWriter(); err != nil {
							fmt.Printf("Error: %v\n", err)
							continue
						}
						fmt.Println("Output restored to stdout.")
						continue
					}
					path := trimTrailingSemicolon(args[0])
					if err := setOutputFile(path); err != nil {
						fmt.Printf("Error: %v\n", err)
						continue
					}
					fmt.Printf("Output redirected to %s.\n", path)
					continue

				case cmdLower == "\\conninfo" || cmdLower == "conninfo":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					resp, err := c.MetaConninfo(entry.SessionId)
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
					}
					renderResponse(cmd, resp)
					continue

				case cmdLower == "desc" || cmdLower == "desc+" || cmdLower == "\\d" || cmdLower == "\\d+":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					if len(args) < 1 {
						fmt.Println("Error: desc requires an object name")
						continue
					}
					objName := trimTrailingSemicolon(args[0])
					if objName == "" {
						fmt.Println("Error: desc requires an object name")
						continue
					}
					detail := "basic"
					if cmdLower == "desc+" || cmdLower == "\\d+" {
						detail = "full"
					}
					resp, err := c.MetaDescribe(entry.SessionId, objName, detail)
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
					}
					renderResponse(cmd, resp)
					continue

				case cmdLower == "\\dt" || cmdLower == "\\dv":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					kind := "table"
					if cmdLower == "\\dv" {
						kind = "view"
					}
					resp, err := c.MetaList(entry.SessionId, kind, "")
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
					}
					renderResponse(cmd, resp)
					continue

				case cmdLower == "\\i" || strings.HasPrefix(cmdName, "@"):
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					fileArg := ""
					if cmdLower == "\\i" {
						if len(args) < 1 {
							fmt.Println("Error: \\i requires a file path")
							continue
						}
						fileArg = args[0]
					} else {
						fileArg = strings.TrimPrefix(cmdName, "@")
						fileArg = strings.TrimSpace(fileArg)
						if fileArg == "" {
							fmt.Println("Error: @ requires a file path")
							continue
						}
					}
					fileArg = trimTrailingSemicolon(fileArg)
					if fileArg == "" {
						fmt.Println("Error: file path is required")
						continue
					}

					content, err := readFileContent("", fileArg)
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
					}
					lines := strings.Split(content, "\n")
					buf := make([]string, 0)
					for _, l := range lines {
						lineText := strings.TrimSpace(l)
						if lineText == "" {
							continue
						}
						buf = append(buf, lineText)
						if strings.HasSuffix(lineText, ";") {
							sql := strings.Join(buf, "\n")
							sql = strings.TrimSuffix(sql, ";")
							buf = nil

							req := &client.ExecuteRequest{
								SessionId: entry.SessionId,
								Sql:       sql,
								Options: client.ExecuteOptions{
									Limit:          0,
									FetchSize:      50,
									QueryTimeoutMs: 0,
								},
							}
							resp, err := c.Execute(req)
							if err != nil {
								fmt.Printf("%v\n", err)
								continue
							}
							renderResponse(cmd, resp)
						}
					}
					if len(buf) > 0 {
						fmt.Println("Warning: trailing statement missing ';' was ignored")
					}
					continue

				case cmdLower == "\\explain" || cmdLower == "explain":
					if shouldRecordHistory(cfg.History.Mode, input, false) {
						line.AppendHistory(input)
					}
					analyze := false
					sql := ""
					if len(args) > 0 {
						sql = strings.Join(args, " ")
					}
					// Accept analyze form: "\\explain analyze <sql>" and alias "explain analyze <sql>"
					if strings.HasPrefix(strings.ToLower(sql), "analyze ") {
						analyze = true
						sql = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(sql), "analyze "))
					}
					// Accept alias form: "explain plan for <sql>"
					if strings.HasPrefix(strings.ToLower(sql), "plan for ") {
						sql = strings.TrimSpace(strings.TrimPrefix(strings.ToLower(sql), "plan for "))
					}
					sql = trimTrailingSemicolon(sql)
					if strings.TrimSpace(sql) == "" {
						fmt.Println("Error: explain requires a SQL statement")
						continue
					}
					resp, err := c.MetaExplain(entry.SessionId, sql, analyze)
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
					}
					renderResponse(cmd, resp)
					continue
				}
			}

			if lower == "/context show" {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				ctxResp, err := c.AiContext(entry.SessionId, 10)
				if err != nil {
					fmt.Printf("%v\n", err)
					continue
				}
				if ctxResp == nil || len(ctxResp.Items) == 0 {
					fmt.Println("No AI context recorded for this session.")
					continue
				}
				for i, item := range ctxResp.Items {
					fmt.Printf("[%d] SQL: %s\n", i+1, item.Sql)
					if strings.TrimSpace(item.Error) != "" {
						fmt.Printf("    ERROR: %s\n", item.Error)
					}
					if len(item.Columns) > 0 {
						fmt.Println("    Columns:")
						for _, col := range item.Columns {
							fmt.Printf("      - %s %s\n", col.Name, col.Type)
						}
					}
					if len(item.SampleRows) > 0 {
						fmt.Println("    Sample rows:")
						for _, row := range item.SampleRows {
							fmt.Printf("      - %v\n", row)
						}
					}
				}
				continue
			}

			if lower == "/context clear" {
				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}
				if err := c.AiContextClear(entry.SessionId); err != nil {
					fmt.Printf("%v\n", err)
					continue
				}
				fmt.Println("AI context cleared.")
				continue
			}

			if strings.HasPrefix(input, "/ai ") {
				multiLineSql = nil
				promptText := strings.TrimSpace(strings.TrimPrefix(input, "/ai "))
				if promptText == "" {
					fmt.Println("Error: /ai requires a prompt")
					continue
				}

				if shouldRecordHistory(cfg.History.Mode, input, false) {
					line.AppendHistory(input)
				}

				aResp, err := c.AiGenerate(&client.AiGenerateRequest{
					Prompt:       promptText,
					DbType:       entry.DbType,
					SessionId:    entry.SessionId,
					ContextMode:  "schema_and_samples",
					ContextLimit: 10,
				})
				if err != nil {
					fmt.Printf("%v\n", err)
					continue
				}

				if len(aResp.Warnings) > 0 {
					for _, w := range aResp.Warnings {
						fmt.Printf("Warning: %s\n", w)
					}
				}

				if strings.TrimSpace(aResp.Sql) == "" {
					fmt.Println("No SQL generated.")
					continue
				}

				type aiStatementsPayload struct {
					Statements []string `json:"statements"`
				}
				var payload aiStatementsPayload
				if err := json.Unmarshal([]byte(aResp.Sql), &payload); err != nil {
					fmt.Println("Error: AI output is not valid JSON.")
					continue
				}
				if len(payload.Statements) == 0 {
					fmt.Println("No SQL statements generated.")
					continue
				}

				statements := make([]string, 0, len(payload.Statements))
				for _, s := range payload.Statements {
					sql := trimTrailingSemicolon(s)
					if strings.TrimSpace(sql) == "" {
						continue
					}
					statements = append(statements, sql)
				}
				if len(statements) == 0 {
					fmt.Println("No SQL statements generated.")
					continue
				}

				fmt.Println("Generated SQL:")
				for i, sql := range statements {
					displaySql := strings.TrimSpace(sql)
					if displaySql != "" && !strings.HasSuffix(displaySql, ";") {
						displaySql += ";"
					}
					fmt.Printf("[%d] %s\n", i+1, displaySql)
				}

				yes, _ := cmd.Flags().GetBool("yes")
				execute := yes
				if !yes {
					confirm, err := line.Prompt("Execute? [Y/n] ")
					if err != nil {
						fmt.Printf("%v\n", err)
						continue
					}
					confirm = strings.TrimSpace(confirm)
					execute = confirm == "" || strings.EqualFold(confirm, "y") || strings.EqualFold(confirm, "yes")
					if strings.EqualFold(confirm, "n") || strings.EqualFold(confirm, "no") {
						execute = false
					}
				}

				if !execute {
					fmt.Println("Aborted.")
					continue
				}

				for i, sql := range statements {
					req := &client.ExecuteRequest{
						SessionId: entry.SessionId,
						Sql:       sql,
						Options: client.ExecuteOptions{
							Limit:          0,
							FetchSize:      50,
							QueryTimeoutMs: 0,
						},
					}

					resp, err := c.Execute(req)
					if err != nil {
						fmt.Printf("Statement [%d] failed: %v\n", i+1, err)
						break
					}
					renderResponse(cmd, resp)
				}
				continue
			}

			multiLineSql = append(multiLineSql, input)

			if strings.HasSuffix(input, ";") {
				sql := strings.Join(multiLineSql, "\n")
				sql = strings.TrimSuffix(sql, ";")
				multiLineSql = nil

				line.AppendHistory(strings.ReplaceAll(sql, "\n", " ") + ";")

				req := &client.ExecuteRequest{
					SessionId: entry.SessionId,
					Sql:       sql,
					Options: client.ExecuteOptions{
						Limit:          0,
						FetchSize:      50,
						QueryTimeoutMs: 0,
					},
				}

				resp, err := c.Execute(req)
				if err != nil {
					fmt.Printf("%v\n", err)
					continue
				}

				renderResponse(cmd, resp)
			}
		}

		if f, err := os.Create(historyPath); err == nil {
			line.WriteHistory(f)
			f.Close()
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(replCmd)
	replCmd.Flags().String("name", "", "Session name to use (tmux-like)")
}
