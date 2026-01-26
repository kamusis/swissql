---
description: Intelligent git commit message workflow that analyzes staged changes and generates structured commit messages
---

## Intelligent Git Commit Message Workflow

### 1. Analyze staged changes

// turbo git status git diff --cached --stat git diff --cached --name-only

### 2. Analyze change types and scope

// turbo git diff --cached --name-only | head -20 git diff --cached | head -50

### 3. Determine commit type and scope

Based on the analysis, determine:

**Commit Types:**

- `feat`: New features, new commands, new endpoints
- `fix`: Bug fixes, error handling improvements
- `refactor`: Code restructuring without functional changes
- `docs`: Documentation updates only
- `chore`: Build process, dependencies, configuration
- `perf`: Performance improvements
- `test`: Test additions or improvements

**Scopes:**

- `cli`: CLI-related changes
- `backend`: Backend API changes
- `repl`: REPL interface changes
- `driver`: Database driver changes
- `sampler`: Sampling system changes
- `config`: Configuration changes

### 4. Generate structured commit message

**Format:**

```
type(scope): concise subject

Major changes:
- Change 1: Brief description
- Change 2: Brief description

Minor improvements:
- Improvement 1: Brief description
```

**Examples:**

**Feature addition:**

```
feat(cli): add DBeaver project import command

Major changes:
- Add import-dbeaver-project command with conflict resolution
- Implement profile filtering and credential encryption

Minor improvements:
- Update README with import examples
- Add driver manifest validation
```

**Bug fix:**

```
fix(repl): resolve connection error message inconsistency

Major changes:
- Fix misleading "Connected successfully!" message on failed connections
- Add proper error handling for empty session IDs

Minor improvements:
- Improve REPL connect command validation
```

**Documentation:**

```
docs(readme): update v0.3.0 feature documentation

Major changes:
- Add comprehensive DBeaver import section
- Document profile management features

Minor improvements:
- Update API endpoints list
- Fix driver manifest example
```

### 5. Commit Message Guidelines

**Subject Line:**

- Use `type(scope): subject` format
- Keep under 50 characters
- Use present tense, imperative mood ("add" not "added")
- Capitalize first letter

**Body:**

- Separate subject from body with blank line
- Use bullet points for multiple changes
- Wrap lines at 72 characters
- Focus on what and why, not how
- Use present tense ("fix" not "fixed")

**Flexibility Guidelines:**

- For simple changes (bug fixes, small tweaks), body is optional
- Single-line commits are fine for minor changes: `fix(ci): correct SQL syntax`
- Only use detailed body when multiple changes or complex modifications
- Avoid over-structuring simple changes

**What to Include:**

- ✅ Major functional changes
- ✅ New features or commands
- ✅ Breaking changes
- ✅ Performance improvements
- ✅ Important refactoring

**What to Exclude:**

- ❌ File URLs or paths
- ❌ Implementation details
- ❌ Minor wording edits
- ❌ Trivial formatting changes
- ❌ Auto-generated content

### 6. Output Format

The workflow must output ONLY the complete commit message without any additional
text, explanations, or metadata.

**Correct Output:**

```
feat(cli): add profile filtering and sorting

Major changes:
- Add db_type and name filtering to list profiles
- Implement alphabetical sorting by db_type then name

Minor improvements:
- Simplify output to show essential fields only
```

**Incorrect Output:**

```
Here is the commit message:
feat(cli): add profile filtering and sorting
...
```

### 7. Usage Examples

**Automatic mode:**

```
/git-commit-message
```

**Manual override (specify type):**

```
/git-commit-message feat
/git-commit-message fix
/git-commit-message docs
```

**Manual override (specify scope):**

```
/git-commit-message cli
/git-commit-message backend
/git-commit-message repl
```
