---
description: Safe branch deletion workflow for all merge scenarios (squash merge, regular merge, and unmerged branches)
---

# Git Safe Delete Branch Workflow

### Overview

This workflow provides a systematic approach to safely delete local branches
after they have been merged via any method (squash merge, regular merge, or
rebase), preventing accidental loss of unmerged work. It is designed to be
compatible with **Windows**, **macOS**, and **Linux**.

### Workflow Steps

#### 1. Check current branch status

**Bash (macOS/Linux/Git Bash):**

```bash
# Detect default branch and switch to it
TARGET_BRANCH="master"
git rev-parse --verify main >/dev/null 2>&1 && TARGET_BRANCH="main"

echo "Using target branch: $TARGET_BRANCH"
git checkout $TARGET_BRANCH
```

**PowerShell (Windows):**

```powershell
# Detect default branch and switch to it
$targetBranch = "master"
if (git rev-parse --verify main 2>$null) { $targetBranch = "main" }

Write-Host "Using target branch: $targetBranch"
git checkout $targetBranch
```

#### 2. Analyze branch differences and merge method

**Bash (macOS/Linux/Git Bash):**

```bash
# Check if branch content is already in target
DIFF_COUNT=$(git log --oneline $TARGET_BRANCH..branch-name | wc -l)
echo "Diff count: $DIFF_COUNT"

# Check merge commit history
git log --oneline --grep="Merge pull request" $TARGET_BRANCH | grep -i "branch-name"
```

**PowerShell (Windows):**

```powershell
# Check if branch content is already in target
$diff = git log --oneline "$($targetBranch)..branch-name"
$diffCount = if ($diff) { ($diff | Measure-Object -Line).Lines } else { 0 }
Write-Host "Diff count: $diffCount"

# Check merge commit history
git log --oneline --grep="Merge pull request" $targetBranch | Select-String "branch-name"
```

#### 3. Safe deletion process

**Bash (macOS/Linux/Git Bash):**

```bash
# Try safe deletion first
git branch -d branch-name || {
    echo "❌ Branch has unique commits not in $TARGET_BRANCH"
    git log --oneline $TARGET_BRANCH..branch-name
    echo "If squash merge is confirmed safe, use: git branch -D branch-name"
}
```

**PowerShell (Windows):**

```powershell
# Try safe deletion first
git branch -d branch-name
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Branch has unique commits not in $targetBranch" -ForegroundColor Yellow
    git log --oneline "$($targetBranch)..branch-name"
    Write-Host "If squash merge is confirmed safe, use: git branch -D branch-name"
}
```

### Git Alias (Cross-Platform)

Add this to your `.gitconfig` for a simplified experience. Git aliases starting
with `!` use the shell bundled with Git (sh.exe on Windows), providing a
consistent environment.

```bash
# One-time setup
git config --global alias.safe-delete '!f() { \
    branch="$1"; \
    if [ -z "$branch" ]; then echo "Usage: git safe-delete branch-name"; exit 1; fi; \
    target="master"; \
    if git rev-parse --verify main >/dev/null 2>&1; then target="main"; fi; \
    if [ "$(git rev-parse --abbrev-ref HEAD)" != "$target" ]; then echo "Please switch to $target branch first"; exit 1; fi; \
    diff_count=$(git log --oneline "$target..$branch" | wc -l); \
    if [ "$diff_count" -eq 0 ]; then \
        echo "✅ Branch content is already included in $target"; \
        git branch -d "$branch"; \
    else \
        echo "❌ Branch has $diff_count commits not in $target"; \
        git log --oneline "$target..$branch"; \
        echo "Use \"git branch -D $branch\" to force delete if verified safe."; \
    fi; \
}; f'

# Usage
git safe-delete branch-name
```

### Decision Tree

| Scenario | Local Diff | Remote Status | Recommended Action                                    |
| -------- | ---------- | ------------- | ----------------------------------------------------- |
| **A**    | 0          | Deleted       | `git branch -d` (Safe)                                |
| **B**    | 0          | Exists        | Verify if PR should be closed, then `git branch -d`   |
| **C**    | > 0        | Deleted       | Check for Squash Merge; if confirmed, `git branch -D` |
| **D**    | > 0        | Exists        | **DO NOT DELETE**. Branch has unmerged work.          |

### Best Practices

1. **Always verify you're on the target branch** (main/master) before deleting
   others.
2. **Never force delete (-D)** unless you've confirmed the work is preserved
   elsewhere (e.g., via a squash merge commit).
3. **Use the alias** to avoid manual parsing errors.
4. **Clean up stale references** regularly: `git remote prune origin`.
