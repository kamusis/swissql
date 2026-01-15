---
description: Workflow to analyze PR comments and provide structured recommendations for review
---

## PR Comments Analysis Workflow

### 1. Check out the PR branch
// turbo
gh pr checkout [id]

### 2. Get comments on PR
// turbo
gh api --paginate repos/[owner]/[repo]/pulls/[id]/comments | jq '.[] | {user: .user.login, body, path, line, original_line, created_at, in_reply_to_id, pull_request_review_id, commit_id}'

### 3. Analyze each comment systematically

For EACH comment, perform the following analysis:

**a. Extract comment information:**
```
(index). From [user] on [file]:[lines] — [comment body]
```

**b. Analyze code context:**
- Read the target file and surrounding code
- Understand the current implementation
- Identify the scope of suggested changes

**c. Feasibility assessment:**
- **High Feasibility**: Simple, clear, low-risk changes
  - Code style/formatting fixes
  - Variable renaming
  - Adding missing error handling
  - Simple logic improvements
  
- **Medium Feasibility**: Moderate complexity changes
  - Refactoring small functions
  - Adding new utility methods
  - Improving error messages
  - Performance optimizations
  
- **Low Feasibility**: Complex or high-risk changes
  - Architectural changes
  - Complex business logic
  - Breaking changes
  - Requires extensive testing

**d. Generate recommendation:**
```
Recommendation: [APPLY/REVIEW/IGNORE]
Reasoning: [Detailed explanation]
Estimated Effort: [LOW/MEDIUM/HIGH]
Code Impact: [SMALL/MEDIUM/LARGE]
Priority: [HIGH/MEDIUM/LOW]
```

### 4. Continue analysis for all comments
Process each comment sequentially without making any code changes.

### 5. Generate comprehensive summary

**Summary Report:**
```
## PR Comments Analysis Summary

### Statistics
- Total Comments Reviewed: [number]
- Recommended to Apply: [number]
- Recommended to Ignore: [number]
- Requires Further Discussion: [number]

### Recommended Changes (by priority)

#### HIGH Priority - Apply First
1. [Comment #] - [Brief description] - Effort: [LOW/MEDIUM/HIGH]
   - File: [file_path]
   - Reason: [why it's important]
   - Impact: [code scope]

#### MEDIUM Priority - Consider Applying
1. [Comment #] - [Brief description] - Effort: [LOW/MEDIUM/HIGH]
   - File: [file_path]
   - Reason: [why it's beneficial]
   - Impact: [code scope]

#### LOW Priority - Optional
1. [Comment #] - [Brief description] - Effort: [LOW/MEDIUM/HIGH]
   - File: [file_path]
   - Reason: [nice to have]
   - Impact: [code scope]

### Recommended to Ignore
1. [Comment #] - [Brief description]
   - Reason: [why to ignore]
   - Alternative: [suggested approach if any]

### Requires Discussion
1. [Comment #] - [Brief description]
   - Issue: [what's unclear]
   - Questions: [what to ask reviewer]

### Implementation Plan
**Phase 1 (Quick Wins):** [list of HIGH priority, LOW effort items]
**Phase 2 (Moderate Changes):** [list of MEDIUM priority items]
**Phase 3 (Consider Later):** [list of LOW priority items]
```

### 6. Usage Examples

**Basic analysis:**
```
/address-pr-comments 123
```

**Specific owner/repo:**
```
/address-pr-comments owner/repo/456
```

### 7. Analysis Criteria

**Feasibility Factors:**
- **Clarity**: How clear is the comment's intent?
- **Risk**: What's the risk of introducing bugs?
- **Scope**: How much code needs to be changed?
- **Testing**: How much testing is required?
- **Dependencies**: Will it affect other components?

**Priority Factors:**
- **Impact**: How much does it improve the code?
- **Urgency**: Is it blocking the PR merge?
- **Standards**: Does it violate coding standards?
- **Performance**: Does it affect performance?
- **Security**: Does it address security concerns?

### 8. Safety Guidelines

**Never Auto-Apply:**
- This workflow only analyzes and recommends
- All code changes require manual approval
- Complex changes always require discussion

**Quality Assurance:**
- Verify understanding before recommending
- Consider edge cases and implications
- Assess testing requirements
- Evaluate impact on existing functionality

### 9. Integration with Development Workflow

**Pre-merge Checklist:**
- [ ] Review HIGH priority recommendations
- [ ] Apply agreed-upon changes
- [ ] Test modified functionality
- [ ] Update documentation if needed
- [ ] Respond to reviewer comments

**Post-merge Follow-up:**
- [ ] Address MEDIUM priority items in future PRs
- [ ] Consider LOW priority items for tech debt
- [ ] Document any architectural decisions
- [ ] Update coding standards if needed

## Implementation Notes

- This workflow focuses on analysis and recommendation only
- No automatic code modifications are performed
- All changes require explicit user approval
- Provides structured, actionable feedback
- Helps prioritize review comments efficiently
- Reduces cognitive load in PR review process