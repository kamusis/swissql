---
description: SwissQL repo git release workflow (Git Bash) that detects current tags and recommends the next cli/backend tags
---

## Git Release (SwissQL)

### Prerequisites

- This workflow is SwissQL-repo-specific and assumes the tag convention:
  - `cli/vX.Y.Z`
  - `backend/vX.Y.Z`
- Run commands in **Git Bash** (recommended on Windows via Git for Windows). These steps use Bash-style command substitution and common GNU tools (`grep`, `sort`, `tail`).
- This workflow does not execute tagging/pushing automatically. It produces **copy-paste** commands for you to run after review.

### 1. Quick sanity checks (repo-specific, deterministic)

// turbo
git status
git log --oneline --merges -10
git log --oneline -10

### 2. Identify latest SwissQL tags (repo-specific, deterministic)

// turbo
git tag -l | grep -E "^(cli|backend)/v[0-9]+\.[0-9]+\.[0-9]+$" | sort -V | tail -10

### 3. Recommend next version + tag commands (repo-specific)

This workflow auto-recommends the next `cli/vX.Y.Z` and `backend/vX.Y.Z` tags and generates single-line `-m` messages.

Rules:

- Tag discovery ignores malformed tags like `cli/vv...` (only `cli/v<semver>` and `backend/v<semver>` are considered).
- Commit scanning is **component-scoped**:
  - CLI message and bump inputs only consider commits that touch `swissql-cli/`.
  - Backend message and bump inputs only consider commits that touch `swissql-backend/`.
- If there are **no** CLI/backend changes since the last tags, the bump stays `patch`.
- New repo fallback: if there are no component tags at all, it scans the last 50 commits of the repo.

Run the script below in **Git Bash** and copy-paste the printed commands.

```bash
set -euo pipefail

latest_semver_from_tag() {
  echo "$1" | sed -E 's@^[^/]*/v@@'
}

semver_bump() {
  local version="$1"
  local bump="$2"
  local major minor patch
  major=$(echo "$version" | cut -d. -f1)
  minor=$(echo "$version" | cut -d. -f2)
  patch=$(echo "$version" | cut -d. -f3)
  if [ "$bump" = "major" ]; then
    major=$((major + 1))
    minor=0
    patch=0
  elif [ "$bump" = "minor" ]; then
    minor=$((minor + 1))
    patch=0
  else
    patch=$((patch + 1))
  fi
  echo "${major}.${minor}.${patch}"
}

latest_cli_tag=$(git tag -l | grep -E '^cli/v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1 || true)
latest_backend_tag=$(git tag -l | grep -E '^backend/v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1 || true)

echo "LATEST_CLI_TAG=${latest_cli_tag}"
echo "LATEST_BACKEND_TAG=${latest_backend_tag}"

base_version=""
if [ -n "${latest_cli_tag}" ] && [ -n "${latest_backend_tag}" ]; then
  base_version=$(printf "%s\n%s\n" "$(latest_semver_from_tag "${latest_cli_tag}")" "$(latest_semver_from_tag "${latest_backend_tag}")" | sort -V | tail -1)
elif [ -n "${latest_cli_tag}" ]; then
  base_version="$(latest_semver_from_tag "${latest_cli_tag}")"
elif [ -n "${latest_backend_tag}" ]; then
  base_version="$(latest_semver_from_tag "${latest_backend_tag}")"
else
  base_version="0.0.0"
fi

echo "BASE_VERSION=${base_version}"

combined_subjects=""
if [ -n "${latest_cli_tag}" ] && git rev-parse -q --verify "${latest_cli_tag}" >/dev/null 2>&1; then
  combined_subjects+="$(git log --pretty=format:"%s" "${latest_cli_tag}..HEAD" -- swissql-cli)"$'\n'
else
  combined_subjects+="$(git log --pretty=format:"%s" -50 -- swissql-cli)"$'\n'
fi
if [ -n "${latest_backend_tag}" ] && git rev-parse -q --verify "${latest_backend_tag}" >/dev/null 2>&1; then
  combined_subjects+="$(git log --pretty=format:"%s" "${latest_backend_tag}..HEAD" -- swissql-backend)"$'\n'
else
  combined_subjects+="$(git log --pretty=format:"%s" -50 -- swissql-backend)"$'\n'
fi

no_component_changes="false"
if [ -z "${combined_subjects//[$'\n\t ']/}" ]; then
  no_component_changes="true"
fi

if [ "${no_component_changes}" = "true" ] && [ -z "${latest_cli_tag}" ] && [ -z "${latest_backend_tag}" ]; then
  combined_subjects="$(git log --pretty=format:"%s" -50)"$'\n'
  no_component_changes="false"
fi

bump_type="patch"
if echo "${combined_subjects}" | grep -Eq 'BREAKING CHANGE|^[a-zA-Z]+\(.+\)!:|^[a-zA-Z]+!:'; then
  bump_type="major"
elif echo "${combined_subjects}" | grep -Eq '^feat(\(.+\))?:'; then
  bump_type="minor"
fi

echo "NO_COMPONENT_CHANGES=${no_component_changes}"

next_version="$(semver_bump "${base_version}" "${bump_type}")"
echo "RECOMMENDED_BUMP_TYPE=${bump_type}"
echo "RECOMMENDED_NEXT_VERSION=${next_version}"

cli_message=""
if [ -n "${latest_cli_tag}" ] && git rev-parse -q --verify "${latest_cli_tag}" >/dev/null 2>&1; then
  cli_message="$(git log --pretty=format:"%s" "${latest_cli_tag}..HEAD" -- swissql-cli | grep -Ev '^(chore\(windsurf\)|chore\(chore\)|docs\(chore\))' | head -n 5 | paste -sd '; ' -)"
else
  cli_message="$(git log --pretty=format:"%s" -50 -- swissql-cli | grep -Ev '^(chore\(windsurf\)|chore\(chore\)|docs\(chore\))' | head -n 5 | paste -sd '; ' -)"
fi
if [ -z "${cli_message}" ]; then
  cli_message="no CLI changes detected"
fi
cli_message="CLI: ${cli_message}."

backend_message=""
if [ -n "${latest_backend_tag}" ] && git rev-parse -q --verify "${latest_backend_tag}" >/dev/null 2>&1; then
  backend_message="$(git log --pretty=format:"%s" "${latest_backend_tag}..HEAD" -- swissql-backend | grep -Ev '^(chore\(windsurf\)|chore\(chore\)|docs\(chore\))' | head -n 5 | paste -sd '; ' -)"
else
  backend_message="$(git log --pretty=format:"%s" -50 -- swissql-backend | grep -Ev '^(chore\(windsurf\)|chore\(chore\)|docs\(chore\))' | head -n 5 | paste -sd '; ' -)"
fi
if [ -z "${backend_message}" ]; then
  backend_message="no backend changes detected"
fi
backend_message="Backend: ${backend_message}."

echo ""
echo "--- COPY-PASTE COMMANDS ---"
echo "git tag -a cli/v${next_version} -m \"${cli_message}\""
echo "git tag -a backend/v${next_version} -m \"${backend_message}\""
echo "git push origin cli/v${next_version} backend/v${next_version}"
```

### 4. Final verification (repo-specific)

After you run the tag/push commands, verify:

```bash
git tag -l | grep -E "^(cli|backend)/vX\.Y\.Z$"
git show cli/vX.Y.Z --quiet
git show backend/vX.Y.Z --quiet
```

## Version determination guidelines (SwissQL)

### Major (X.0.0)

- Breaking API changes
- Major architectural refactoring
- Database driver compatibility changes
- Removal of deprecated features

### Minor (X.Y.0)

- New CLI commands
- New backend endpoints
- New database driver support
- Significant UX improvements
- New integrations

### Patch (X.Y.Z)

- Bug fixes and error handling improvements
- Documentation updates
- Code refactoring and optimizations
- Minor UX enhancements

## Usage

Run this repo-specific workflow:
```
/git-release-swissql
```