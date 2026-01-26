---
description: Comprehensive workflow for managing GitHub issues using the github-issues skill
---

# GitHub Issue Management Workflow

## 0. Prerequisites Check

Verify that the `github-issues` skill is available in one of the configuration
directories.

// turbo ls .agent/skills/github-issues/SKILL.md
.windsurf/skills/github-issues/SKILL.md .qoder/skills/github-issues/SKILL.md
.github/skills/github-issues/SKILL.md 2>/dev/null || echo "ERROR: github-issues
skill not found"

> **Note**: If no SKILL.md file is found in any of the supported directories
> (.agent, .windsurf, .qoder, or .github), please notify the user that they need
> to install the `github-issues` skill before using this workflow.

## 1. Create New Issues

Use this when you need to document new work, bugs, or ideas.

### Bug Report

**Action**: Create a detailed bug report.

- **Template**: Use the `Bug Report Template`.
- **Details**: Include "Steps to Reproduce", "Expected Behavior", and
  "Environment".
- **Labels**: Add `bug`.

### Feature Request

**Action**: Create a feature request for new functionality.

- **Template**: Use the `Feature Request Template`.
- **Details**: Include "Motivation", "Proposed Solution", and "Acceptance
  Criteria".
- **Labels**: Add `enhancement`.

### General Task / Chore

**Action**: Create a task for refactoring, documentation, or maintenance.

- **Template**: Use the `Task Template`.
- **Details**: Include "Objective" and "Checklist".

## 2. Update Existing Issues

Use this to maintain issue accuracy and track progress.

### Modify Content & Metadata

- **Update**: Call `mcp_issue_write` with the existing `issue_number`.
- **Preservation**: Always call `mcp_issue_read` first to ensure you don't
  overwrite existing body content unless intended.
- **Fields**: Update `title`, `body`, `labels`, `assignees`, or `milestone`.

### Change Status

- **Close**: Use `state: "closed"` when a task is completed.
- **Reopen**: Use `state: "open"` if a fix failed or more work is needed.

## 3. Communication

### Add Comments

- **Action**: Use `mcp_add_issue_comment` to provide status updates, link PRs,
  or ask questions.
- **Context**: Mention specific technical details or reference other issues/PRs
  using `#number`.

## 4. Discovery & Retrieval

### Search & List

- **Global Search**: Use `mcp_search_issues` to find issues across title, body,
  and comments using keywords.
- **Repository List**: Use `mcp_list_issues` to see the current state of the
  project (e.g., "List all open bug issues").

### Read Details

- **Action**: Use `mcp_issue_read` to get the full body and metadata of a
  specific issue before performing updates or responding.

## 5. Usage Examples

**Create a new feature request:**

```
/git-issue "Create a feature request for a dark mode theme"
```

**Search for related issues:**

```
/git-issue "Find all issues related to connmgr credentials"
```

**Close a completed issue:**

```
/git-issue "Close issue #16 as the enhancements are now implemented"
```

**Add a comment to an issue:**

```
/git-issue "Add a comment to #12: 'Investigated the logs and found a race condition.'"
```

## Tips for Success

- **Be Specific**: In titles, use prefixes like `[Bug]` or `[Feat]`.
- **Reference Everything**: Use `#number` to link related items.
- **Label Diligently**: Labels make discovery much easier for project
  maintainers.
