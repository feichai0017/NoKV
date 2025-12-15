# Contributing to NoKV

Thank you for your interest in contributing to NoKV! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)
- [Reporting Bugs](#reporting-bugs)
- [Suggesting Enhancements](#suggesting-enhancements)

## Code of Conduct

We expect all contributors to be respectful and professional. Please:

- Be welcoming to newcomers
- Be respectful of differing viewpoints and experiences
- Accept constructive criticism gracefully
- Focus on what's best for the community
- Show empathy towards other community members

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Set up the development environment
4. Create a feature branch
5. Make your changes
6. Test your changes
7. Submit a pull request

## Development Setup

### Prerequisites

- Go 1.23 or higher
- Git
- Docker and Docker Compose (for integration tests)
- Make (optional, but recommended)

### Clone and Build

```bash
# Clone your fork
git clone https://github.com/YOUR-USERNAME/NoKV.git
cd NoKV

# Add upstream remote
git remote add upstream https://github.com/feichai0017/NoKV.git

# Install dependencies
go mod download

# Build the project
make build
# or
go build -v ./...
```

### Running Tests

```bash
# Run all tests
make test

# Run tests with race detector
make test-race

# Run tests with coverage
make test-coverage

# Run short tests (faster)
make test-short
```

### Running the Local Cluster

```bash
# Option 1: Using Make
make local-cluster

# Option 2: Using script directly
./scripts/run_local_cluster.sh --config ./raft_config.example.json

# Option 3: Using Docker Compose
make docker-up
```

## Making Changes

### Branch Naming

Use descriptive branch names:

- `feature/your-feature-name` - for new features
- `fix/issue-description` - for bug fixes
- `docs/what-you-document` - for documentation updates
- `refactor/what-you-refactor` - for code refactoring

### Commit Messages

Follow these guidelines for commit messages:

```
<type>: <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Example:**
```
feat: add TTL support for Redis SET command

Implement TTL (Time To Live) support for the Redis SET command
to enable automatic key expiration. This includes:
- TTL metadata storage
- Background expiration cleanup
- Integration with MVCC transactions

Fixes #123
```

### Code Organization

- Keep changes focused and atomic
- One logical change per commit
- Break large features into smaller, reviewable PRs
- Update documentation alongside code changes

## Testing

### Test Requirements

All contributions must include appropriate tests:

- **Unit tests**: Test individual functions/methods
- **Integration tests**: Test component interactions
- **Benchmarks**: For performance-critical code

### Writing Tests

```go
func TestYourFeature(t *testing.T) {
    // Setup
    db := setupTestDB(t)
    defer db.Close()
    
    // Execute
    result, err := db.YourFeature()
    
    // Assert
    assert.NoError(t, err)
    assert.Equal(t, expected, result)
}
```

### Running Specific Tests

```bash
# Run tests in a specific package
go test ./lsm/...

# Run a specific test
go test -run TestYourFeature ./pkg/...

# Run with verbose output
go test -v ./...
```

## Code Style

### Go Code Style

Follow standard Go conventions:

- Use `gofmt` for formatting (enforced in CI)
- Follow [Effective Go](https://golang.org/doc/effective_go.html)
- Use meaningful variable and function names
- Add comments for exported functions and types
- Keep functions focused and small (prefer < 50 lines)
- Handle errors explicitly, don't ignore them

### Format Your Code

```bash
# Format all Go files
make fmt

# or manually
gofmt -w -s .
go mod tidy
```

### Linting

```bash
# Install golangci-lint
make install-tools

# Run linter
make lint
```

### Code Comments

- Add godoc comments for all exported types and functions
- Use `//` for single-line comments
- Use `/* */` for multi-line comments or package documentation
- Explain "why" not "what" in comments

**Example:**
```go
// CompactLevel performs a compaction on the specified level to reduce
// read amplification and reclaim space from deleted entries.
// It merges overlapping SSTables and writes the result to the next level.
func (lsm *LSM) CompactLevel(level int) error {
    // ...
}
```

## Submitting Changes

### Before Submitting

Ensure your changes:

1. ✅ Build successfully (`make build`)
2. ✅ Pass all tests (`make test`)
3. ✅ Pass linting (`make lint`)
4. ✅ Include appropriate tests
5. ✅ Update relevant documentation
6. ✅ Follow code style guidelines
7. ✅ Have clear commit messages

### Pull Request Process

1. **Update your fork**
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Push to your fork**
   ```bash
   git push origin your-branch-name
   ```

3. **Create Pull Request**
   - Go to the NoKV repository on GitHub
   - Click "New Pull Request"
   - Select your fork and branch
   - Fill out the PR template
   - Link related issues

4. **PR Description**
   Include:
   - What changes were made
   - Why the changes are needed
   - How to test the changes
   - Any breaking changes
   - Related issue numbers

5. **Review Process**
   - Maintainers will review your PR
   - Address feedback and questions
   - Make requested changes
   - Once approved, maintainers will merge

### PR Title Format

```
<type>: <description>
```

Examples:
- `feat: add support for range queries`
- `fix: resolve memory leak in value log GC`
- `docs: update installation instructions`

## Reporting Bugs

### Before Reporting

- Check existing issues to avoid duplicates
- Test with the latest version
- Gather relevant information

### Bug Report Template

```markdown
**Description**
Clear description of the bug

**To Reproduce**
Steps to reproduce:
1. Step 1
2. Step 2
3. ...

**Expected Behavior**
What you expected to happen

**Actual Behavior**
What actually happened

**Environment**
- NoKV version:
- Go version:
- OS:
- Architecture:

**Additional Context**
- Logs
- Configuration
- Stack traces
```

## Suggesting Enhancements

### Enhancement Proposal Template

```markdown
**Feature Description**
Clear description of the proposed feature

**Motivation**
Why this feature is needed

**Proposed Implementation**
How you propose to implement it

**Alternatives Considered**
Other approaches you've considered

**Additional Context**
- Use cases
- Examples
- References
```

## Documentation

### Documentation Updates

When making changes that affect:

- **API**: Update godoc comments
- **Features**: Update relevant docs in `docs/`
- **Configuration**: Update configuration examples
- **CLI**: Update `docs/cli.md`
- **Architecture**: Update `docs/architecture.md`

### Writing Documentation

- Use clear, concise language
- Include code examples
- Add diagrams where helpful (Mermaid syntax supported)
- Update the table of contents

## Getting Help

- **Questions**: Open a discussion on GitHub Discussions
- **Issues**: Open an issue with the bug or feature template
- **Chat**: (Add chat link if available)

## Recognition

Contributors will be recognized in:

- Release notes
- CONTRIBUTORS file (if created)
- GitHub contributors page

Thank you for contributing to NoKV! 🚀
