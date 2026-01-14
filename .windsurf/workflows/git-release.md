---
description: Intelligent git release workflow that analyzes recent changes and auto-generates version tags and release notes
---

## Intelligent Git Release Workflow

### 1. Analyze recent merge history and changes
// turbo
git log --oneline --merges -5
git log --oneline -5
git status

### 2. Analyze changes since last release
// turbo
# Find the most recent tag
LATEST_TAG=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [ -n "$LATEST_TAG" ]; then
    echo "Analyzing changes since last tag: $LATEST_TAG"
    git log --pretty=format:"%h %s" $LATEST_TAG..HEAD
else
    echo "No previous tags found, analyzing last 10 commits"
    git log --oneline -10
fi

### 3. Check current tags to determine next version
// turbo
git tag -l | grep -E "(cli|backend)/v[0-9]+\.[0-9]+\.[0-9]+" | sort -V | tail -5

### 4. Analyze README.md and key files for feature changes
// turbo
git log --since="1 week ago" -- README.md CHANGELOG.md
git diff HEAD~5..HEAD --name-only | grep -E "\.(go|java|md)$"

### 5. Determine version type and next version number
Based on the analysis above, determine if this is:
- **Major (X.0.0)**: Breaking changes, major architectural changes
- **Minor (X.Y.0)**: New features, significant enhancements  
- **Patch (X.Y.Z)**: Bug fixes, minor improvements, documentation updates

### 6. Generate intelligent release notes
Analyze the merged PRs and changes to create comprehensive release notes covering:
- Major features added
- Backend changes (new endpoints, driver support, etc.)
- CLI changes (new commands, UX improvements)
- Technical improvements and refactoring
- Documentation updates

### 7. Generate copy-paste ready git tag commands
Based on the analysis above, here are the exact commands to run:

```bash
git tag -a cli/v{next_version} -m "{auto_generated_release_notes}"
git tag -a backend/v{next_version} -m "{auto_generated_release_notes}"
git push origin cli/v{next_version} backend/v{next_version}
```

**Review the commands above, then run them in your preferred shell.**

**To verify the release worked, run these commands after:**
```bash
git tag -l | grep v{next_version}
git log --oneline -3
```

## Version Determination Logic

### Major Version (X.0.0)
- Breaking API changes
- Major architectural refactoring
- Database driver compatibility changes
- Removal of deprecated features

### Minor Version (X.Y.0)  
- New CLI commands
- New backend endpoints
- New database driver support
- Significant UX improvements
- New integrations (DBeaver import, etc.)

### Patch Version (X.Y.Z)
- Bug fixes and error handling improvements
- Documentation updates
- Code refactoring and optimizations
- Minor UX enhancements
- Security updates

## Auto-Generated Release Notes Template

**🚀 Major Features**
- [Feature 1]: Brief description based on commit messages
- [Feature 2]: Brief description based on commit messages

**🔧 Backend Changes**
- [Change 1]: New endpoints, driver support, etc.
- [Change 2]: Performance improvements, refactoring

**💻 CLI Changes**  
- [Change 1]: New commands, improved UX
- [Change 2]: Better error handling, validation

**🛠️ Technical Improvements**
- [Improvement 1]: Code quality, testing, etc.
- [Improvement 2]: Dependencies, build process

**📚 Documentation**
- [Update 1]: README updates, new examples
- [Update 2]: API documentation, guides

## Usage Examples

### Automatic mode (let AI decide everything):
```
/git-release
```

### Manual override (specify version type):
```
/git-release minor  # Force minor version bump
/git-release patch  # Force patch version bump
/git-release major  # Force major version bump
```

### Custom version:
```
/git-release 0.4.0  # Specify exact version
```

## Implementation Notes

- The workflow analyzes the last week of changes by default
- Uses semantic versioning based on change types
- Automatically generates comprehensive release notes
- Creates both CLI and backend tags with identical versions
- Verifies successful tag creation and pushing
- Handles edge cases like no recent changes or version conflicts