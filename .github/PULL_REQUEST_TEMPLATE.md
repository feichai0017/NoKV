## Description

<!-- Provide a brief description of the changes in this PR -->

## Type of Change

<!-- Mark the relevant option with an "x" -->

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Code refactoring
- [ ] Test improvements
- [ ] CI/CD changes

## Related Issues

<!-- Link to related issues -->

Fixes #(issue number)
Relates to #(issue number)

## Changes Made

<!-- Provide a detailed description of your changes -->

- Change 1
- Change 2
- Change 3

## Testing

<!-- Describe the tests you ran to verify your changes -->

### Test Configuration

- **Go version**: 
- **OS**: 
- **Test commands run**:

```bash
make test
make test-race
```

### Test Results

<!-- Paste relevant test output or screenshots -->

```
# Test output
```

## Performance Impact

<!-- If applicable, describe any performance implications -->

- [ ] No performance impact
- [ ] Performance improvement (provide benchmarks)
- [ ] Performance regression (explain why it's acceptable)

### Benchmarks (if applicable)

```
# Before
BenchmarkOperation-8   1000000   1234 ns/op

# After  
BenchmarkOperation-8   1500000    890 ns/op
```

## Documentation

<!-- Check all that apply -->

- [ ] Code is self-documenting with clear variable/function names
- [ ] Added/updated godoc comments for exported functions
- [ ] Updated relevant documentation in `docs/`
- [ ] Updated README.md (if needed)
- [ ] Updated CHANGELOG.md (if applicable)
- [ ] No documentation needed

## Checklist

<!-- Ensure you have completed the following -->

- [ ] My code follows the project's code style guidelines
- [ ] I have run `make fmt` to format the code
- [ ] I have run `make lint` and addressed all issues
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally with my changes (`make test`)
- [ ] I have run tests with race detector (`make test-race`)
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] My changes generate no new warnings
- [ ] Any dependent changes have been merged and published

## Breaking Changes

<!-- If this PR introduces breaking changes, list them here -->

- [ ] No breaking changes

**If there are breaking changes:**

- What breaks:
- Migration guide:
- Deprecation notice period:

## Screenshots (if applicable)

<!-- Add screenshots to help explain your changes -->

## Additional Notes

<!-- Add any additional notes, concerns, or context for reviewers -->

## Reviewer Notes

<!-- Specific areas you'd like reviewers to focus on -->

- Please pay special attention to:
- Questions for reviewers:
