---
name: git-pr-creator
description: 'Generate PR title and description by analyzing git commits and code changes. Reads differences between current branch and master/main, extracts commit messages, reviews modified files, and produces a structured PR description for user review. Does NOT automatically create the PR - returns content for manual review.'
---

# Git PR Creator

Analyze git commits and code changes to automatically generate PR title and description.

## What I Do

1. **Analyze commits** - Read all commits in current branch that differ from `master` or `main`
2. **Extract commit context** - Parse commit messages and identify scope of changes
3. **Review code changes** - Examine modified files to understand implementation details
4. **Generate PR content** - Create structured PR title and description
5. **Return for review** - Present the generated content to user for approval before creating PR

## When to Use Me

Triggers on requests like:
- "Generate a PR for my changes"
- "Create PR description from commits"
- "Summarize my branch as a PR"
- "Draft PR for current branch"
- "Generate PR title and description"

## Workflow

### Step 1: Detect Current Branch and Target Branch

```bash
# Get current branch name
git rev-parse --abbrev-ref HEAD

# Identify default branch (master or main)
git symbolic-ref refs/remotes/origin/HEAD
```

### Step 2: Get Commit Differences

```bash
# List all commits not in target branch
git log master..HEAD --oneline
# or
git log main..HEAD --oneline

# Get detailed commit info
git log master..HEAD --format="%H|%an|%ae|%ad|%s|%b"
```

### Step 3: Analyze Code Changes

```bash
# Get list of changed files
git diff master..HEAD --name-status

# Get diff stats
git diff master..HEAD --stat

# View specific file changes for context
git show <commit>:<file>
```

### Step 4: Generate PR Content

#### PR Title Generation Rules

1. **Type prefix** (optional): `[Feature]`, `[Fix]`, `[Refactor]`, `[Docs]`, `[Test]`
2. **Subject**: Clear, concise description of main change
3. **Length**: Keep under 72 characters
4. **Format**: `[Type] Short description` or just `Short description`

**Examples:**
- `[Feature] Add PostgreSQL 15 collector YAML support`
- `[Fix] Correct execution plan query for pg_stat_statements`
- `Refactor sampler initialization logic`

#### PR Description Structure

```markdown
## Description
[One-paragraph summary of changes and their purpose]

## Changes
- [Bullet point for each major change]
- [Group related changes together]

## Type of Change
- [ ] Bug fix (non-breaking fix)
- [ ] New feature (non-breaking addition)
- [ ] Breaking change (fix or feature causing existing functionality to change)
- [ ] Documentation update

## How to Test
[Optional: Steps to verify the changes work correctly]

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Comments added for complex logic
- [ ] Documentation updated
- [ ] Tests added/updated (if applicable)

## Related Issues
[Reference any related GitHub issues with #issue_number]

## Commits
[List of commits included:]
- commit_hash: commit_message
```

### Step 5: Present for User Review

1. Display generated PR title
2. Display full PR description
3. Show list of commits included
4. Show summary of files changed
5. Ask user to review and approve
6. **Do NOT** automatically create the PR - wait for explicit user confirmation

## Example Workflow

**User**: "Generate a PR for my changes"

**Process**:
1. Check current branch (e.g., `feature/pg-collectors`)
2. Find commits since `main`:
   - `a1b2c3d: Add PostgreSQL 15 collector-top.yaml`
   - `d4e5f6g: Add PostgreSQL 15 swiss-15.yaml`
   - `h7i8j9k: Fix sqlplan query for PostgreSQL`
3. Analyze changed files:
   - `swissql-backend/jdbc_drivers/postgres/collector-15-top.yaml` (new)
   - `swissql-backend/jdbc_drivers/postgres/swiss-15.yaml` (new)
   - `swissql-backend/jdbc_drivers/postgres/collector-15.yaml` (modified)
4. Generate:
   ```
   PR Title: [Feature] Add PostgreSQL 15 TOP and Swiss collectors
   
   PR Description:
   ## Description
   Adds PostgreSQL 15 database collectors configuration following the 4-layer performance metrics model (Context, Resource, Wait, Load Attribution). Splits collector-15.yaml into two YAML files: collector-15-top.yaml for TOP metrics and swiss-15.yaml for Swiss sampler queries.
   
   ## Changes
   - Created collector-15-top.yaml with 6-layer TOP collector (context, cpu, sessions, waits, topSessions, io)
   - Created swiss-15.yaml with 13 Swiss sampler queries (sqltext, sqlplan, active_transactions, locks, etc.)
   - Added YAML schema completeness (order, render_hint fields)
   - Verified all SQL queries execute correctly on PostgreSQL 15
   
   ## Type of Change
   - [x] New feature (non-breaking addition)
   - [x] Documentation update
   
   Files changed (3):
   - swissql-backend/jdbc_drivers/postgres/collector-15-top.yaml (new)
   - swissql-backend/jdbc_drivers/postgres/swiss-15.yaml (new)
   - swissql-backend/jdbc_drivers/postgres/collector-15.yaml (modified)
   
   Commits included (3):
   - a1b2c3d Add PostgreSQL 15 collector-top.yaml
   - d4e5f6g Add PostgreSQL 15 swiss-15.yaml
   - h7i8j9k Fix sqlplan query for PostgreSQL
   ```

5. Return to user for review and approval

## Important Notes

- **Does NOT create PR automatically** - Only generates content
- **Respects user's review** - User must explicitly approve before any PR action
- **Handles multiple commits** - Aggregates messages from all commits in branch
- **Identifies default branch** - Automatically detects `master` or `main`
- **Shows commit context** - Displays commits included for transparency
- **Lists changed files** - Shows all modified/added/deleted files
- **Markdown formatted** - Returns properly formatted PR description ready for GitHub

## Technical Details

### Git Commands Used

| Command | Purpose |
|---------|---------|
| `git rev-parse --abbrev-ref HEAD` | Get current branch name |
| `git symbolic-ref refs/remotes/origin/HEAD` | Detect default branch |
| `git log master..HEAD` | List commits in current branch |
| `git diff master..HEAD` | Get file changes |
| `git show <commit>:<file>` | View specific file content |

### Commit Message Parsing

- **First line**: Commit summary (becomes part of description)
- **Blank line**: Separator
- **Body**: Additional context (included if present)

### File Change Analysis

- **Added files** (`A`): New functionality
- **Modified files** (`M`): Changes to existing code
- **Deleted files** (`D`): Removed functionality
- **Renamed files** (`R`): Code reorganization

## Error Handling

- If no commits found: Inform user branch is even with target
- If working directory dirty: Suggest stashing changes first
- If default branch detection fails: Ask user to specify target branch
- If commit parsing fails: Show raw commit output for manual review
