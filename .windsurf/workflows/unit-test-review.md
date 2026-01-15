---
description: Unit test review workflow for validating test quality, coverage, and trustworthiness (multi-language)
---

## Unit Test Review Workflow

### 1. Detect language and analyze test structure
// turbo
# Detect project language
if [ -f "go.mod" ]; then
  echo "Language: Go"
  TEST_EXT="*_test.go"
  TEST_CMD="go test ./..."
  COVERAGE_CMD="go test ./... -coverprofile=coverage.out"
elif [ -f "pom.xml" ]; then
  echo "Language: Java"
  TEST_EXT="*Test.java"
  TEST_CMD="mvn test"
  COVERAGE_CMD="mvn jacoco:report"
elif [ -f "package.json" ]; then
  echo "Language: JavaScript/TypeScript"
  TEST_EXT="*.test.js *.test.ts *.spec.js *.spec.ts"
  TEST_CMD="npm test"
  COVERAGE_CMD="npm test -- --coverage"
elif [ -f "requirements.txt" ] || [ -f "pyproject.toml" ]; then
  echo "Language: Python"
  TEST_EXT="test_*.py *_test.py"
  TEST_CMD="pytest"
  COVERAGE_CMD="pytest --cov=. --cov-report=term"
else
  echo "Unknown language"
  exit 1
fi

# Find all test files
find . -type f \( -name "$TEST_EXT" \) | head -20

# Count test files vs source files
echo "=== Source Files ==="
find . -type f -name "*.go" -o -name "*.java" -o -name "*.py" -o -name "*.js" -o -name "*.ts" | grep -v "_test.go" | grep -v "Test.java" | grep -v "test_" | grep -v ".test." | grep -v ".spec." | wc -l
echo "=== Test Files ==="
find . -type f \( -name "$TEST_EXT" \) | wc -l

# Check test coverage
COVERAGE_DIR=$(mktemp -d)
echo $COVERAGE_CMD | sh 2>&1 | grep -E "(coverage|PASS|FAIL|Coverage)" | head -20
# Clean up coverage files
rm -rf "$COVERAGE_DIR"

### 2. Run tests and check for false positives
// turbo
# Create temporary directory for test output
TEMP_DIR=$(mktemp -d)
TEST_OUTPUT_LOG="$TEMP_DIR/test_output.log"

# Run all tests with verbose output
echo $TEST_CMD | sh -v 2>&1 | tee "$TEST_OUTPUT_LOG"

# Check for tests that pass but have warnings
grep -i "warning\|deprecated\|todo\|fixme" "$TEST_OUTPUT_LOG" | head -20

# Check for tests that skip important scenarios
grep -i "skip\|t.skip\|@skip\|@disabled\|pytest.skip" "$TEST_OUTPUT_LOG" | head -20

# Clean up temporary files
rm -rf "$TEMP_DIR"

### 3. Analyze test quality patterns
// turbo
# Check for tests that only test success paths
grep -r "func Test.*Success\|func Test.*HappyPath\|def test.*success\|def test.*happy.*path\|test.*Success\|it.*should.*succeed" --include="*_test.go" --include="*Test.java" --include="test_*.py" --include="*.test.js" --include="*.test.ts" | head -10

# Check for tests that don't test error paths
grep -r "if err != nil\|catch.*error\|except.*Error\|\.catch\|expect.*toThrow" --include="*_test.go" --include="*Test.java" --include="test_*.py" --include="*.test.js" --include="*.test.ts" | wc -l
grep -r "t.Error\|t.Fatalf\|assert.*fail\|assertThat.*fail\|assert.*error\|expect.*error" --include="*_test.go" --include="*Test.java" --include="test_*.py" --include="*.test.js" --include="*.test.ts" | wc -l

# Check for tests with empty or minimal assertions
grep -r "t.Error(\"expected\|t.Errorf(\"expected\|assertThat\|expect\|assert.*equal" --include="*_test.go" --include="*Test.java" --include="test_*.py" --include="*.test.js" --include="*.test.ts" | wc -l

### 4. Check test synchronization with code
// turbo
# Find functions that changed recently but have no tests
git log --since="1 month ago" --pretty=format:"%h" --name-only | grep -E "\.(go|java|py|js|ts)$" | grep -v "_test.go\|Test.java\|test_.*\.py\|\.test\.\|\.spec\." | sort -u | head -20

# Check for outdated test comments
grep -r "TODO.*test\|FIXME.*test\|XXX.*test" --include="*_test.go" --include="*Test.java" --include="test_*.py" --include="*.test.js" --include="*.test.ts" | head -10

# Check for commented-out test code
grep -r "// t\.\|// assert\|// expect\|# assert\|# expect" --include="*_test.go" --include="*Test.java" --include="test_*.py" --include="*.test.js" --include="*.test.ts" | head -10

### 5. Generate review checklist

**Test Trustworthiness Review:**
- [ ] Tests actually execute the code being tested (not just mock everything)
- [ ] Tests verify side effects (file writes, database changes, network calls)
- [ ] Tests use real data structures, not just interfaces
- [ ] Tests check return values AND error conditions
- [ ] Tests verify state changes, not just return values
- [ ] Tests don't rely on implementation details (test behavior, not internals)
- [ ] Tests are deterministic (no random data or time-dependent logic)
- [ ] Tests clean up after themselves (no resource leaks)

**Test Coverage Review:**
- [ ] Coverage report shows >70% for critical paths
- [ ] Error paths are tested (not just success paths)
- [ ] Edge cases are tested (empty inputs, nil values, boundary conditions)
- [ ] Integration tests cover real interactions
- [ ] Tests cover all public API functions
- [ ] Tests cover all major code branches
- [ ] Coverage is tracked over time (not decreasing)
- [ ] Uncovered code has justification (e.g., deprecated features)

**Test Synchronization Review:**
- [ ] Tests match current code structure (no references to deleted functions)
- [ ] Test names clearly describe what they test
- [ ] Test data reflects current business rules
- [ ] Tests use current API signatures
- [ ] Tests reference current config/environment variables
- [ ] No commented-out test code
- [ ] No TODO/FIXME comments in tests
- [ ] Tests updated when code changes

**Test Quality Review:**
- [ ] Tests are readable and maintainable
- [ ] Tests use table-driven patterns for multiple cases
- [ ] Tests have clear setup/execute/verify structure
- [ ] Tests use descriptive assertion messages
- [ ] Tests isolate concerns (one test = one scenario)
- [ ] Tests use helpers to reduce duplication
- [ ] Tests don't depend on execution order
- [ ] Tests run quickly (no unnecessary sleeps or delays)

**Test Anti-Patterns Review:**
- [ ] No tests that only check if function doesn't panic
- [ ] No tests that only check if function returns non-nil
- [ ] No tests with empty or minimal assertions
- [ ] No tests that skip all assertions
- [ ] No tests that mock everything (no real code execution)
- [ ] No tests that rely on global state
- [ ] No tests that are too broad (testing too much at once)
- [ ] No tests that are implementation-dependent

### 6. Review specific test patterns

**Good Test Patterns (Go):**
```go
// Table-driven tests with clear cases
func TestFunction(t *testing.T) {
    tests := []struct {
        name    string
        input   InputType
        want    OutputType
        wantErr bool
    }{
        {"valid input", validInput, expectedOutput, false},
        {"invalid input", invalidInput, nil, true},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            got, err := Function(tt.input)
            if (err != nil) != tt.wantErr {
                t.Errorf("Function() error = %v, wantErr %v", err, tt.wantErr)
            }
            if got != tt.want {
                t.Errorf("Function() = %v, want %v", got, tt.want)
            }
        })
    }
}
```

**Good Test Patterns (Java):**
```java
@Test
public void testFunction() {
    // Valid input
    assertEquals(expectedOutput, Function(validInput));

    // Invalid input
    assertThrows(IllegalArgumentException.class, () -> Function(invalidInput));
}
```

**Good Test Patterns (Python):**
```python
def test_function():
    # Valid input
    assert function(valid_input) == expected_output

    # Invalid input
    with pytest.raises(ValueError):
        function(invalid_input)
```

**Good Test Patterns (JavaScript/TypeScript):**
```javascript
test('function', () => {
    // Valid input
    expect(function(validInput)).toBe(expectedOutput);

    // Invalid input
    expect(() => function(invalidInput)).toThrow();
});
```

**Bad Test Patterns (Any Language):**
```go
// ❌ Tests that only check if function doesn't panic
func TestFunction(t *testing.T) {
    Function(input)
}

// ❌ Tests with empty assertions
func TestFunction(t *testing.T) {
    got := Function(input)
    if got != nil {
        t.Log("got something")
    }
}

// ❌ Tests that mock everything
func TestFunction(t *testing.T) {
    mock := NewMockEverything()
    result := Function(mock)
    assert.NotNil(t, result) // Only checks mock returns something
}
```

### 7. Check for common test issues

// turbo
# Check for tests that don't verify results
grep -A 5 "func Test" --include="*_test.go" | grep -E "^\s*$" | wc -l

# Check for tests with only one assertion
grep -r "t.Error\|t.Errorf" --include="*_test.go" -A 1 | grep -E "^\s*$" | wc -l

# Check for tests that use t.Skip excessively
grep -r "t.Skip\|t.Skipf" --include="*_test.go" | wc -l

# Check for tests with sleep/delay
grep -r "time.Sleep\|sleep" --include="*_test.go" | wc -l

### 8. Verify test data and fixtures

// turbo
# Check for hardcoded test data that might be outdated
grep -r "202[0-9]-[0-9]{2}" --include="*_test.go" | head -10

# Check for test data files
find . -name "*testdata*" -o -name "*fixtures*" | head -10

# Check for test data that references external services
grep -r "http://\|https://\|localhost:\|127.0.0.1:" --include="*_test.go" | head -10

### 9. Generate test quality report

**Test Quality Metrics:**
- Total test files: ___
- Total test functions: ___
- Average tests per file: ___
- Code coverage: ___%
- Tests with error path coverage: ___%
- Tests with edge case coverage: ___%
- Tests using table-driven pattern: ___%
- Tests with clear assertions: ___%
- Tests that skip: ___%
- Tests with sleep/delay: ___%

**Critical Issues:**
- Tests that don't verify results: ___
- Tests with empty assertions: ___
- Tests that mock everything: ___
- Tests that are implementation-dependent: ___
- Tests that rely on global state: ___
- Tests that are outdated: ___

**Recommendations:**
1.
2.
3.

### 10. Specific review questions

**For each test file, ask:**
1. Does this test actually execute the code being tested?
2. Does this test verify both success and error paths?
3. Does this test check side effects (file writes, DB changes)?
4. Does this test use realistic test data?
5. Does this test have clear assertions with helpful messages?
6. Does this test clean up after itself?
7. Does this test run quickly and deterministically?
8. Does this test match the current code structure?

**For each critical function, ask:**
1. Is there a test for this function?
2. Does the test cover all branches?
3. Does the test cover error conditions?
4. Does the test cover edge cases?
5. Is the test updated when the function changes?

### 11. Integration test review

**Integration Test Checklist:**
- [ ] Integration tests use real dependencies (not mocks)
- [ ] Integration tests test real workflows
- [ ] Integration tests verify end-to-end behavior
- [ ] Integration tests are isolated (don't depend on each other)
- [ ] Integration tests have proper setup/teardown
- [ ] Integration tests use test databases/fixtures
- [ ] Integration tests are marked with build tags
- [ ] Integration tests are documented

### 12. Test documentation review

**Test Documentation Checklist:**
- [ ] Test files have package-level documentation
- [ ] Test functions have clear names describing what they test
- [ ] Test comments explain WHY, not WHAT
- [ ] Test data is documented
- [ ] Test setup/teardown is documented
- [ ] Test helper functions are documented
- [ ] Test examples are provided for complex scenarios

### 13. Generate final recommendations

**Immediate Actions (This Sprint):**
1.
2.
3.

**Short-term Actions (Next 2-4 Sprints):**
1.
2.
3.

**Long-term Actions (Next Quarter):**
1.
2.
3.

### 14. Test quality scorecard

**Scorecard:**
- Test Trustworthiness: ___/10
- Test Coverage: ___/10
- Test Synchronization: ___/10
- Test Quality: ___/10
- Test Anti-Patterns: ___/10

**Overall Score: ___/50**

**Grade:**
- 45-50: Excellent
- 40-44: Good
- 35-39: Fair
- 30-34: Poor
- <30: Critical

### 15. Best practices enforcement

**Enforce These Rules:**
- Every public function must have a test
- Every error path must be tested
- Tests must verify both return values and side effects
- Tests must use table-driven patterns for multiple cases
- Tests must have clear assertion messages
- Tests must clean up resources
- Tests must not use sleep/delay
- Tests must be deterministic

**Automated Checks:**
```bash
# Create temporary directory for coverage files
TEMP_DIR=$(mktemp -d)

# Run with coverage (Go example - adapt for other languages)
if [ -f "go.mod" ]; then
  go test ./... -coverprofile="$TEMP_DIR/coverage.out" -covermode=atomic
  go tool cover -func="$TEMP_DIR/coverage.out" | tail -20
elif [ -f "pom.xml" ]; then
  mvn jacoco:report
elif [ -f "package.json" ]; then
  npm test -- --coverage
elif [ -f "requirements.txt" ] || [ -f "pyproject.toml" ]; then
  pytest --cov=. --cov-report=term
fi

# Check for tests that don't verify results
echo $TEST_CMD | sh -v 2>&1 | grep "PASS" | xargs -I {} sh -c 'grep -A 10 "{}" *_test.go | grep -c "t.Error\|t.Errorf" || echo "No assertions"'

# Check test coverage trends
git log --since="1 month ago" --pretty=format:"%h %s" | grep "test\|coverage"

# Clean up temporary files
rm -rf "$TEMP_DIR"
```

### 16. Continuous improvement

**Track These Metrics Over Time:**
- Test coverage percentage
- Number of tests
- Test execution time
- Number of flaky tests
- Number of skipped tests
- Test quality score

**Review Frequency:**
- Before each major release
- After significant code changes
- Monthly for ongoing projects
- Quarterly for comprehensive review
