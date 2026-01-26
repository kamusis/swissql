---
description: Intelligent PR generation workflow that analyzes changes and draft PR title and description
---

# Git Pull Request Workflow

## 0. Prerequisites Check

Verify that the `git-pr-creator` skill is available in one of the configuration
directories.

// turbo ls .agent/skills/git-pr-creator/SKILL.md
.windsurf/skills/git-pr-creator/SKILL.md .qoder/skills/git-pr-creator/SKILL.md
.github/skills/git-pr-creator/SKILL.md 2>/dev/null || echo "ERROR:
git-pr-creator skill not found"

> **Note**: If no SKILL.md file is found in any of the supported directories
> (.agent, .windsurf, .qoder, or .github), please notify the user that they need
> to install the `git-pr-creator` skill before using this workflow.

## 1. Analyze Branch State

// turbo git rev-parse --abbrev-ref HEAD git symbolic-ref
refs/remotes/origin/HEAD

### Determine Diff Range

Identify the commits that belong to this branch compared to the target branch
(usually `main` or `master`).

// turbo git log origin/main..HEAD --oneline git diff origin/main..HEAD --stat

## 2. Review Implementation Details

Examine the actual code changes to understand the technical impact and
implementation choices. This feeds into the "Description" and "Changes" sections
of the PR.

// turbo git diff origin/main..HEAD --name-only | head -30

## 3. Generate PR Content

### Create Draft

Generate structured content based on:

1. **Commit Messages**: Primary source for what was done.
2. **Code Diff**: Secondary source for how it was done.
3. **Change Impact**: Analysis of affected modules.

### Structure

- **Title**: `[Type] Short description` (under 72 chars)
- **Description**: Summary paragraph explaining the "why".
- **Changes**: Bulleted list of technical modifications.
- **Type of Change**: Categorization (Bug fix, Feature, etc.).
- **How to Test**: Specific verification steps.
- **Related Issues**: Reference tracked items (e.g., `Fixes #16`).

## 4. User Review & Approval

Present the following to the user for confirmation:

1. **Suggested PR Title**
2. **Full Markdown Description**
3. **List of Included Commits**
4. **Summary of Files Changed**

> **Note**: This workflow only _generates_ the content. It does not
> automatically submit the PR to GitHub unless you explicitly confirm the final
> draft.

## 5. Usage Examples

**Generate PR for current work:**

```
/git-pr
```

**Generate PR targeting specific branch:**

```
/git-pr develop
```

**Include related issue reference:**

```
/git-pr "Draft PR for #16"
```

## Checklist before Submission

- [ ] Commits are squashed or follow clean history
- [ ] No temporary debug code or logs left behind
- [ ] All new functions have docstrings
- [ ] Tests passed locally
- [ ] Related issue matches the branch purpose
